package ch3batch.lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object RDDs {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)


}
