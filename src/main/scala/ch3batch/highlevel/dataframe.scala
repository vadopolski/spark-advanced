package ch3batch.highlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.io.{BufferedSource, Source}

object dataframe extends App {

  /**
   * 1. Abstraction with a set of Physical operator
   *
   * Why RDD is under the hood DataFrame?
   *
   *
   *
   * */

  import org.apache.spark.sql.functions._


  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val tenNumsDF = spark.range(0, 10).localCheckpoint

  val grouped = tenNumsDF
    .filter(col("id") > 0)
    .select(pmod(col("id"), lit(2)).alias("mod2"))
    .groupBy(col("mod2").cast("int").alias("pmod2")).count

  grouped.count

  grouped.show(20, false)
  grouped.explain(true)

  /**
   * == Parsed Logical Plan ==
   * 'Aggregate [cast('mod2 as int) AS pmod2#9], [cast('mod2 as int) AS pmod2#9, count(1) AS count#12L]
   * +- Project [pmod(id#0L, cast(2 as bigint)) AS mod2#7L]
   * +- Filter (id#0L > cast(0 as bigint))
   * +- LogicalRDD [id#0L], false
   *
   * */

  val parser = spark.sessionState.sqlParser
  val plan: LogicalPlan = parser.parsePlan("SELECT nothing_function(id) FROM foo WHERE near = 0")


  /**
   * == Analyzed Logical Plan ==
   * pmod2: int, count: bigint
   * Aggregate [cast(mod2#7L as int)], [cast(mod2#7L as int) AS pmod2#9, count(1) AS count#12L]
   * +- Project [pmod(id#0L, cast(2 as bigint)) AS mod2#7L]
   * +- Filter (id#0L > cast(0 as bigint))
   * +- LogicalRDD [id#0L], false
   *
   *
   * column checking
   * function checking
   * column and fumction checking
   *
   *
   * */


  /**
   *
   * == Optimized Logical Plan ==
   * Aggregate [_groupingexpression#35], [_groupingexpression#35 AS pmod2#9, count(1) AS count#12L]
   * +- Project [cast(pmod(id#0L, 2) as int) AS _groupingexpression#35]
   * +- Filter (id#0L > 0)
   * +- LogicalRDD [id#0L], false
   *
   * Catalyst Optimiser
   * More than 40 optimisations
   *
   */

  /**
   * == Physical Plan ==
   * AdaptiveSparkPlan isFinalPlan=false
   * +- HashAggregate(keys=[_groupingexpression#35], functions=[count(1)], output=[pmod2#9, count#12L])
   * +- HashAggregate(keys=[_groupingexpression#35], functions=[partial_count(1)], output=[_groupingexpression#35, count#31L])
   * +- Project [cast(pmod(id#0L, 2) as int) AS _groupingexpression#35]
   * +- Filter (id#0L > 0)
   * +- Scan ExistingRDD[id#0L]
   *
   *
   * Physical Operator
   * case class FilterExec(condition: Expression, child: SparkPlan)
   * case class CoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode {
   *
   * All extended from UnaryExecNode
   * All has field - SparkPlan - abstract class with two methods:
   *
   * final def execute(): RDD[InternalRow] = call doExecute which has all logic from physical operator
   * Returns the result of this query as an RDD[InternalRow] by delegating to `doExecute` after
   * preparations. Concrete implementations of SparkPlan should override `doExecute`.
   *
   *
   * protected def doExecute(): RDD[InternalRow]
   * Produces the result of the query as an `RDD[InternalRow]`
   * Overridden by concrete implementations of SparkPlan
   *
   *
   * Each doExecute() has child.execute() - recursive call of previous physical operator

   * == Physical Plan ==
   * AdaptiveSparkPlan isFinalPlan=false
   * +- HashAggregate(keys=[_groupingexpression#35],            <-------------- child.execute()
   * +- Project [cast(pmod(id#0L, 2) as int)                    <-------------- child.execute()
   * +- Filter (id#0L > 0)                                      <-------------- child.execute() with !!!! condition from code
   * +- Scan ExistingRDD[id#0L]
   *
   * Each doExecute() has the result of working prev operator and return RDD[InternalRow] --- where do we get the first element of the row? see next
   * All physical operator is a chain of transformation of RDD[InternalRow]
   *
   * For example FilterExec has predicate and partitions as Iterator of InternalRow
   * and we apply iterator to each element of Iterator
   *
   * Questions ??????
   *
   * Iterator()  - lazy collection to work with a large dataset,  init element in the memory calling .next
   *
   *
   * where do we get the first element of the row? see next
   *
   *
   *
   *
   *
   */

  val it = Iterator(1,2,3,4)
  val list = List(1,2,3,4)

  val source: BufferedSource = Source.fromFile("")
  source.next()

  /**
   *
   * abstract class Source extends Iterator[Char] with Closeable {
      /** the actual iterator */
   *  protected val iter: Iterator[Char]
   *
   * */

  /**
   * DATAFRAME creating
   *
   * where do we get the first element of the row? see next
   *
   * First physical operator from plan which create first RDD[InternalRaw]
   *
   * - Scan ExistingRDD[id#0L]
   *
   * Each DataFrame created from
   *
   * DataSource API v 1 (in Spark 1 & 2) or DataSource API v 2 (in Spark 2)
   *
   * All DataFrame have to be extended from 'abstract class BaseRelation' which has a two important methods:
   * 'def schema: StructType' - generate a schema, this method is called in Driver and produce the schema of DataFrame
   *
   * 'def buildScan(): RDD[Row]' - build data of dataframe which is RDD of InternalRaw
   *
   * Each time when you execute spark.read.format("xxx").load
   * methods 'schema' and 'buildScan' create DataFrame
   *
   * RDD[InternalRaw] - dont have a schema
   *
   * Each time when you execute spark.write.format("xxx").save()
   * Under the hood there is a
   *
   * rdd.foreachPartition { rows =>
   * write(rows)
   * }
   *
   *
   * methods 'schema' and 'buildScan' create DataFrame
   *
   * Where RDD has type RDD[InternalRow], rows - Iterator[InternalRow],  write - function which write partition to target
   * executed on each worker
   *
   * */

  /**
   * How to create RDD[InternalRaw]
   * Each RDD looks like
   *
   * abstract class RDD[T] extends Serializable with Logging {
   *      def compute(split: Partition): Iterator[T] -- executed on execute
   *      def getPartitions: Array[Partition] -- executed on driver
   *      }
   *  trait Partition extends Serializable {
   *  def index: Int
   *  }
   *
   * And each RDD has two methods 'def compute' 'def getPartitions' method is called on the driver and returns
   * description of partitions:
   * - amount of partitions
   * - each partition has the order number
   * - each partition has the mapping of blocks from sources to partition
   * - all partitions are sent to executer
   *
   * Each Partition object (concrete implementation) has all necessary information, enough to read data from block
   * in HDFS in worker
   *
   * def compute(split: Partition):
   * def getPartitions: Array[Partition]:
   * Only when we use action .count || .collect methods getPartitions and compute works only when
   *
   *
   * */



}
