package mod3highlevel

import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DemoDataSet extends App {
  /**
   * Create the first DataSet from the actual trip data file in Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
   * Using DSL and lambda, build a table that will show - How is the distribution of trips by distance?
   * Display the result on the screen and write it to the Postgres database (docker in the project).
   * To write to the database, you need to think over and also attach an init sql file with a structure.
   *
   * (Example: you can build a storefront with the following columns: total trips, average distance, standard deviation,
   * minimum and maximum distance)
   *
   * Result: Data with the resulting table should appear in the console, a table should appear in the database.
   * Check out the solution in github gist.
   *
   * */


  import mod3highlevel.model._

  val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local[2]")
    .getOrCreate()

  import spark.implicits._


  val taxiFactsDF: DataFrame =
    spark.read
      .load("src/main/resources/yellow_taxi_jan_25_2018")

  val taxiFactsDS: Dataset[TaxiRide] =
    taxiFactsDF
      .as[TaxiRide]

  val taxiZoneDS: Dataset[TaxiZone] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/taxi_zones.csv")
    .as[TaxiZone]

  val result: Dataset[Row] = taxiFactsDS
    .join(broadcast(taxiZoneDS), col("DOLocationID") === col("LocationID"), "left")
    .as[Result]
    //    show the plan because its not optimised
    //    .filter(x => x.DOLocationID != 0)
    .filter(col("DOLocationID") =!= 0)
    .groupBy(col("Borough"))
    .count()
    .orderBy(col("count").desc)

  result.explain(true)

  result
    .show()

}
