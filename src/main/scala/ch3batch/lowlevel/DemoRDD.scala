package ch3batch.lowlevel

import ch3batch.model._
import org.apache.spark.{Dependency, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, desc, desc_nulls_first}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object DemoRDD extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  /**
   * Load data into RDD from parquet with an actual trip data file (src/main/resources/data/yellow_taxi_jan_25_2018).
   * Use lambda to build a table that shows what time the most calls occur.
   * Output the result to the screen and to a txt file with spaces.
   * Result: Data with the resulting table should appear in the console, a file should appear in the file system.
   * Check the solution in github gist.
   * */

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._
  import ch3batch.model._

  val value = sc.parallelize('a' to 'z', 6)

  val taxiZoneRDD = sc.textFile("src/main/resources/taxi_zones.csv")

  val header: String = taxiZoneRDD.first()

  val filteredTaxiZoneRDD: RDD[TaxiZone] = taxiZoneRDD
    .filter(r => r != header)
    .map(l => l.split(","))
    .map(t => TaxiZone(t(0).toInt, t(1), t(2), t(3)))

  val taxiFactsDF: DataFrame =
    spark.read
      .load("src/main/resources/yellow_taxi_jan_25_2018")

  val taxiFactsDS: Dataset[TaxiRide] =
    taxiFactsDF.as[TaxiRide]

  val taxiFactsRDD: RDD[TaxiRide] =
    taxiFactsDS.rdd


  val mappedTaxiZoneRDD: RDD[(Int, String)] = filteredTaxiZoneRDD
    .map(z => (z.LocationID, z.Borough))

  val mappedTaxiFactRDD: RDD[(Int, Int)] =
    taxiFactsRDD
      .map(x => (x.DOLocationID, 1))

  val joinedAndGroupedRDD: RDD[(String, Int)] =
    mappedTaxiFactRDD
      .join(mappedTaxiZoneRDD)
      .map {
        case (key, (cnt, bor)) => (bor, cnt)
      }
      .reduceByKey {
        (prev, next) => prev + next
      }
      .sortBy(_._2, false)

  println(joinedAndGroupedRDD.toDebugString)

  joinedAndGroupedRDD
    .take(10)
    .foreach(x => println(x))

  Thread.sleep(100000)


}
