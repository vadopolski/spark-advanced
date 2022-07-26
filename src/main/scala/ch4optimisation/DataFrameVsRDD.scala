package ch4optimisation

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.lang

object DataFrameVsRDD extends App {
  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  /**
   * How to create DataFrame?
   * */

  implicit val sc = spark.sparkContext

  /*
  println(s"RDD")
  val startTime: Long = System.currentTimeMillis
  val rdd = sc.parallelize(1 to 100000000)
  val initRddFinishTime: Long = System.currentTimeMillis
  rdd.cache()
  println(rdd.count())
  val rddCountTime: Long = System.currentTimeMillis
  calcTimeDiff(initRddFinishTime, startTime, rddCountTime)
*/

  /**
   * ParallelCollectionRDD	Memory Deserialized 1x Replicated	1	100%	381.5 MiB	0.0 B
   */


  /**
   * println(s"DataSet to RDD")
   * val startDataFrameTime2: Long = System.currentTimeMillis
   * val ds2: Dataset[lang.Long] = spark.range(1, 100000000)
   * val initDFTime2: Long = System.currentTimeMillis
   * val rdd = ds2.rdd
   * println(rdd.count())
   * val countDFTime2: Long = System.currentTimeMillis
   * calcTimeDiff(initDFTime2, startDataFrameTime2, countDFTime2)
   */


  println(s"DataSet")
  val startDataFrameTime: Long = System.currentTimeMillis
  val ds: Dataset[lang.Long] = spark.range(1, 100000005)
  val initDFTime: Long = System.currentTimeMillis
  ds.persist(StorageLevel.MEMORY_ONLY_SER)
  println(ds.count())
  val countDFTime: Long = System.currentTimeMillis
  calcTimeDiff(initDFTime, startDataFrameTime, countDFTime)

  /**
   * 4	*(1) Range (1, 100000005, step=1, splits=1)	Disk Memory Deserialized 1x Replicated	1	100%	97.5 MiB	0.0 B
   * */

  /**
   *
   * 4	*(1) Range (1, 100000005, step=1, splits=1)	Memory Serialized 1x Replicated	1	100%	96.5 MiB	0.0 B
   * */


  private def calcTimeDiff(initRddFinish: Long, start: Long, rddCount: Long) = {
    println(s"Diff init is ${initRddFinish - start}")
    println(s"Diff count is ${rddCount - start}")
  }

  Thread.sleep(100000)

}
