package ch3batch

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadWriteUtils {
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)


  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)


}
