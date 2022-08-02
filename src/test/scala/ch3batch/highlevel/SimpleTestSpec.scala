package ch3batch.highlevel

import mod3highlevel.DemoDataFrame.processTaxiData
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SimpleTestSpec extends AnyFlatSpec  {
  import mod3highlevel.ReadWriteUtils._

  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1 for Big Data Application")
    .getOrCreate()

  it should "Successfully calculate the distribution by valid taxi data" in {
    val taxiZonesDF = readCSV("src/test/resources/taxi_zones.csv")
    val taxiDF = readParquet("src/test/resources/yellow_taxi_jan_25_2018")

    val actualDistribution = processTaxiData(taxiZonesDF, taxiDF)
      .collectAsList()
      .get(0)

    assert(actualDistribution.get(0) == "Manhattan")
    assert(actualDistribution.get(1) == 296529)
    assert(actualDistribution.get(2) == 0.0)
    assert(actualDistribution.get(3) == 2.18)
    assert(actualDistribution.get(4) == 37.92)
  }
}
