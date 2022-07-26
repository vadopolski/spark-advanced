package ch3batch.highlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{broadcast, col, count, desc_nulls_first, max, mean, min, round, to_json}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel.DISK_ONLY

import java.util.Properties


object DemoDataFrame extends App {

  /**
   * Create first DataFrame from the Parquet actual trip data file (src/main/resources/data/yellow_taxi_jan_25_2018).
   * Load data into a second DataFrame from a csv trip reference file (src/main/resources/data/taxi_zones.csv).
   * Using DSL and two this dataframes build a table that will show which areas are the most popular for bookings.
   * Display the result on the screen and write it to the Parquet file.
   *
   * Result: Data with the resulting table should appear in the console, a file should appear in the file system.
   *
   * */

  import ch3batch.ReadWriteUtils._

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()


  /**
   * How to create DataFrame?
   * */

  val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018").persist(DISK_ONLY)
  val taxiZoneDF: DataFrame = readCSV("src/main/resources/taxi_zones.csv").cache()

  taxiFactsDF.count()
  taxiZoneDF.count()

  implicit val sc = spark.sparkContext

  val result: DataFrame = processTaxiData(taxiFactsDF, taxiZoneDF)

  /**
   * How to check to see all execution plans
   * */
  result.explain(true)

  result.show()

  /**
   * How to get all dependencies from DataFrame?
   * */
  val rdd: RDD[InternalRow] = result.queryExecution.toRdd
  println(s"current dependencies is ${rdd.dependencies}")
  println(s"current dependencies is ${rdd.dependencies.head.rdd.dependencies}")



  processTaxiDataSQL(taxiFactsDF, taxiZoneDF).show()

  Thread.sleep(10000000)


  def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame): DataFrame =
    taxiFactsDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .count()
      .orderBy(col("count").desc)

  def processTaxiDataSQL(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame): DataFrame = {
    taxiFactsDF.createOrReplaceTempView("taxi_facts")
    taxiZoneDF.createOrReplaceTempView("taxi_Zone")

    spark.sql(
      """
        |SELECT count(*) as count
        |      FROM taxi_facts
        |      JOIN taxi_Zone ON DOLocationID = LocationID
        |GROUP BY Borough
    """.stripMargin)
  }


}
