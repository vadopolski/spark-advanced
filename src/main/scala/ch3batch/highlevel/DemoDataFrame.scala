package ch3batch.highlevel

import org.apache.spark.sql.functions.{broadcast, col, count, desc_nulls_first, max, mean, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel.DISK_ONLY

import java.util.Properties


object DemoDataFrame extends App {

  import ch3batch.ReadWriteUtils._

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame): DataFrame =
    taxiFactsDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total trips"),
        round(min("trip_distance"), 2).as("min distance"),
        round(mean("trip_distance"), 2).as("mean distance"),
        round(max("trip_distance"), 2).as("max distance")
      )
      .orderBy(col("total trips").desc)

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

  val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_from_db").persist(DISK_ONLY)
  val taxiZoneDF: DataFrame = readCSV("src/main/resources/taxi_zones.csv").cache()

  taxiFactsDF.count()
  taxiZoneDF.count()

  val result: DataFrame = processTaxiData(taxiFactsDF, taxiZoneDF)

  result.explain(true)

  result.show()

  processTaxiDataSQL(taxiFactsDF, taxiZoneDF).show()

  Thread.sleep(10000000)


}
