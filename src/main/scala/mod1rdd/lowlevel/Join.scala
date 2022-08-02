package mod1rdd.lowlevel

import org.apache.spark.sql.SparkSession

object Join extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext


  /**
   * Exercise:
   * 1. Traverse join/cogroup transformation back through dependencies and check it with .toDebygString
   *
   * 2. Make join without shuffle dependencies
   * */


  val firstRDD = sc.parallelize(1 to 100, 5).keyBy(x => x * x)
  //  firstRDD: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[40] at keyBy at <console>:24

  val secondRDD = sc.parallelize(50 to 250, 10).keyBy(_ % 11)
  //    secondRDD: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[42] at keyBy at <console>:24

  firstRDD.getNumPartitions
  //  res21: Int = 5

  secondRDD.getNumPartitions
  //  res22: Int = 10


  firstRDD.cogroup(secondRDD).dependencies
  //      res20: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.OneToOneDependency@12cb2246)


  firstRDD.cogroup(secondRDD).dependencies(0).rdd
  //  res25: org.apache.spark.rdd.RDD[_] = CoGroupedRDD[47] at cogroup at <console>:28

  firstRDD.cogroup(secondRDD).dependencies(0).rdd.dependencies
  //  res26: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.ShuffleDependency@6fe81817, org.apache.spark.ShuffleDependency@6e513171)

  firstRDD.cogroup(secondRDD).dependencies(0).rdd.dependencies(0)
  //  res27: org.apache.spark.Dependency[_] = org.apache.spark.ShuffleDependency@5a9056db

  firstRDD.cogroup(secondRDD).dependencies(0).rdd.dependencies(1)
  //   res28: org.apache.spark.Dependency[_] = org.apache.spark.ShuffleDependency@efc72b6


  /**
   *
   * Solution 1
   *
   * Solution 2 - recommendation
   *
   * 1) Build with equal two RDD with equal partitioner
   * 2) Add to join num of partitions .join(rdd2, 15)
   * 3) Try to execute with .partitionBy() and val p = new HashPartitioner(10)
   *
   * */




}
