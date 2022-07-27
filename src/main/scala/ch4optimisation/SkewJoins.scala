package ch4optimisation

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{abs, array, avg, col, explode}

object SkewJoins extends App {

  val spark = SparkSession.builder()
    .appName("Skewed Joins")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()

  import spark.implicits._
  import model._

  val laptopDS = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/laptop/laptop.csv")
    .as[Laptop]

  val laptopOffersDS = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/laptop/laptopOffer.csv")
    .as[LaptopOffer]

  val joined = laptopDS
    .join(laptopOffersDS, List("make", "model"))
    .filter(abs(laptopOffersDS.col("procSpeed") - laptopDS.col("procSpeed")) <= 0.1)
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))


  val laptops2 = laptopDS.withColumn("procSpeed", explode(array($"procSpeed" - 0.1, $"procSpeed", $"procSpeed" + 0.1)))
  val joined2 = laptops2.join(laptopOffersDS, List("make", "model", "procSpeed"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  joined2.show()
  joined2.explain()
  Thread.sleep(1000000)
}
