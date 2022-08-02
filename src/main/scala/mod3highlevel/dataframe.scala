package mod3highlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

import scala.io.{BufferedSource, Source}

object dataframe extends App {

  import org.apache.spark.sql.functions._

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()


  spark.conf.set("spark.sql.adaptive.enabled", false)


  /**
   * Exercise - reverse engineering: read the Query Plans and try to recreate code the code that generated them.
   */

  /**
   * == Physical Plan ==
   * (1) Project [firstName#153, lastName#155, (cast(salary#159 as double) / 1.1) AS salary_EUR#168]
   * +- *(1) FileScan csv [firstName#153,lastName#155,salary#159] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Users/VOpolskiy/IdeaProjects/luxoft-materials/spark-advanced/...], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<firstName:string,lastName:string,salary:string>
   */

  /**
   * == Physical Plan ==
   * (2) HashAggregate(keys=[dept#156], functions=[avg(cast(salary#181 as bigint))])
   * +- Exchange hashpartitioning(dept#156, 200)
   * +- *(1) HashAggregate(keys=[dept#156], functions=[partial_avg(cast(salary#181 as bigint))])
   * +- *(1) Project [dept#156, cast(salary#159 as int) AS salary#181]
   * +- *(1) FileScan csv [dept#156,salary#159] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Users/VOpolskiy/IdeaProjects/luxoft-materials/spark-advanced/...], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<dept:string,salary:string>
   */

  /**
   * == Physical Plan ==
   * (5) Project [id#195L]
   * +- *(5) SortMergeJoin [id#195L], [id#197L], Inner
   * :- *(2) Sort [id#195L ASC NULLS FIRST], false, 0
   * :  +- Exchange hashpartitioning(id#195L, 200)
   * :     +- *(1) Range (1, 10000000, step=3, splits=6)
   * +- *(4) Sort [id#197L ASC NULLS FIRST], false, 0
   * +- Exchange hashpartitioning(id#197L, 200)
   * +- *(3) Range (1, 10000000, step=5, splits=6)
   */


  /**
   * 1. Abstraction with a set of Physical operator
   *
   * Why RDD is under the hood DataFrame?
   *
   *
   *
   * */


  val tenNumsDF = spark.range(0, 10)

  val grouped = tenNumsDF
    .filter(col("id") > 0)
    //    .localCheckpoint()
    .select(pmod(col("id"), lit(2)).alias("mod2"))
    .groupBy(col("mod2").cast("int").alias("pmod2")).count

  grouped.count

  grouped.show(20, false)
  grouped.explain(true)


  /** with local checkpoint
   * == Physical Plan ==
   * (1) HashAggregate(keys=[_groupingexpression#35], functions=[count(1)], output=[pmod2#9, count#12L])
   * +- *(1) HashAggregate(keys=[_groupingexpression#35], functions=[partial_count(1)], output=[_groupingexpression#35, count#31L])
   * +- *(1) Project [cast(pmod(id#0L, 2) as int) AS _groupingexpression#35]
   * +- *(1) Scan ExistingRDD[id#0L]
   *
   * */

  /** without local checkpoint
   * == Physical Plan ==
   * (1) HashAggregate(keys=[_groupingexpression#32], functions=[count(1)], output=[pmod2#6, count#9L])
   * +- *(1) HashAggregate(keys=[_groupingexpression#32], functions=[partial_count(1)], output=[_groupingexpression#32, count#28L])
   * +- *(1) Project [cast(pmod(id#0L, 2) as int) AS _groupingexpression#32]
   * +- *(1) Filter (id#0L > 0)
   * +- *(1) Range (0, 10, step=1, splits=1)
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
   *
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

  val it = Iterator(1, 2, 3, 4)
  val list = List(1, 2, 3, 4)

  val source: BufferedSource = Source.fromFile("")
  source.next()

  /**
   *
   * abstract class Source extends Iterator[Char] with Closeable {
   * /** the actual iterator */
   * protected val iter: Iterator[Char]
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
   * def compute(split: Partition): Iterator[T] -- executed on execute
   * def getPartitions: Array[Partition] -- executed on driver
   * }
   * trait Partition extends Serializable {
   * def index: Int
   * }
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


  /**
   * 5. Spark catalog, sharedstate, session state
   * */

  spark.catalog
  spark.sharedState
  spark.sessionState

  spark.catalog
  //  res0: org.apache.spark.sql.catalog.Catalog = org.apache.spark.sql.internal.CatalogImpl@5410bbdf

  spark.sharedState
  //  res1: org.apache.spark.sql.internal.SharedState = org.apache.spark.sql.internal.SharedState@4f3f8115

  spark.sessionState
  //  res2: org.apache.spark.sql.internal.SessionState = org.apache.spark.sql.internal.SessionState@53c1f4ae

  spark.sharedState.cacheManager
  // cashing data

  spark.sharedState.externalCatalog
  //  res6: org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener = org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener@66c3c813

  spark.sharedState.externalCatalog.listDatabases()
  //  res8: Seq[String] = Buffer(default)


  spark.catalog.listDatabases().show(100, 100)

  /**
   * +-------+---------------------+---------------------------------------------------------------------+
   * |   name|          description|                                                          locationUri|
   * +-------+---------------------+---------------------------------------------------------------------+
   * |default|Default Hive database|        file:/C:/Spark2/spark-3.1.3-bin-hadoop2.7/bin/spark-warehouse|
   * |   test|                     |file:/C:/Spark2/spark-3.1.3-bin-hadoop2.7/bin/spark-warehouse/test.db|
   * +-------+---------------------+---------------------------------------------------------------------+
   * */

  spark.catalog.listTables().show()

  import spark.implicits._

  case class Person(name: String)

  val persons = Seq(Person("Vadim"), Person("Ivan"))
  val personsDS = persons.toDS

  personsDS.write.saveAsTable("managed_table")

  spark.catalog.listTables().show()

  /**
   * +-------------+--------+-----------+---------+-----------+
   * |         name|database|description|tableType|isTemporary|
   * +-------------+--------+-----------+---------+-----------+
   * |managed_table| default|       null|  MANAGED|      false|
   * +-------------+--------+-----------+---------+-----------+
   */

  personsDS.createOrReplaceTempView("external_table")

  spark.catalog.listTables().show()

  /**
   * +--------------+--------+-----------+---------+-----------+
   * |          name|database|description|tableType|isTemporary|
   * +--------------+--------+-----------+---------+-----------+
   * | managed_table| default|       null|  MANAGED|      false|
   * |external_table|    null|       null|TEMPORARY|       true|
   * +--------------+--------+-----------+---------+-----------+
   */

  spark.sessionState.analyzer.postHocResolutionRules
  // responsible for correct column naming

  spark.sessionState.conf.getConf(SQLConf.CBO_ENABLED)

  spark.sessionState.conf.setConf(SQLConf.CBO_ENABLED, true)

  //
  // https://docs.databricks.com/spark/latest/spark-sql/cbo.html


  spark.sessionState.optimizer.nonExcludableRules

  /**
   * res31: Seq[String] = List(org.apache.spark.sql.catalyst.optimizer.EliminateDistinct, org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint,
   * org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases, org.apache.spark.sql.catalyst.analysis.EliminateView,
   * org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions, org.apache.spark.sql.catalyst.optimizer.ComputeCurrentTime,
   * org.apache.spark.sql.catalyst.optimizer.GetCurrentDatabaseAndCatalog, org.apache.spark.sql.catalyst.optimizer.RewriteDistinctAggregates,
   * org.apache.spark.sql.catalyst.optimizer.ReplaceDeduplicateWithAggregate, org.apache.spark.sql.catalyst.optimizer.ReplaceIntersectWithSemiJoin,
   * org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithFilter, org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithAntiJoin...
   * */

  spark.sessionState.optimizer.earlyScanPushDownRules


  /**
   * Seq[org.apache.spark.sql.catalyst.rules.Rule[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]] =
   * List(org.apache.spark.sql.execution.datasources.SchemaPruning$@68b015f8,
   * org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$@3bab8e19,
   * org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions$@1c4a903c,
   * org.apache.spark.sql.hive.execution.PruneHiveTablePartitions@13a688c)
   * */

  // here your can see push down aggregation and filters

  spark.sessionState.planner.extraPlanningStrategies
  //  Seq[org.apache.spark.sql.Strategy] =
  //  List(org.apache.spark.sql.hive.HiveStrategies$HiveTableScans$@3d6e3a16,
  //       org.apache.spark.sql.hive.HiveStrategies$HiveScripts$@49c230e7)


}
