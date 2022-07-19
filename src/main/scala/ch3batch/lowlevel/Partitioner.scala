package ch3batch.lowlevel

import org.apache.spark.sql.SparkSession

object Partitioner extends App {

  /**
   * Partitioner,
   *
   * Sorting Operation - benchmark, sort one gigabyte
   *
   * Slide how to work sorting and RangePartitioner
   *
   * org.apache.spark.Partitioner
   *
   *
   * */

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  import org.apache.spark.Partitioner

  val forSortRDD = sc.parallelize(1 to 100)
  //  forSortRDD: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

  forSortRDD.keyBy(x => x * x)
  // res2: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[1] at keyBy at <console>:26

  forSortRDD.keyBy(x => x * x).collect
  //   res3: Array[(Int, Int)] = Array((1,1), (4,2), (9,3), (16,4), (25,5), (36,6), (49,7), (64,8), (81,9), (100,10), (121,11), (144,12), (169,13), (196,14), (225,15), (256,16), (289,17), (324,18), (361,19), (400,20), (441,21), (484,22), (529,23), (576,24), (625,25), (676,26), (729,27), (784,28), (841,29), (900,30), (961,31), (1024,32), (1089,33), (1156,34), (1225,35), (1296,36), (1369,37), (1444,38), (1521,39), (1600,40), (1681,41), (1764,42), (1849,43), (1936,44), (2025,45), (2116,46), (2209,47), (2304,48), (2401,49), (2500,50), (2601,51), (2704,52), (2809,53), (2916,54), (3025,55), (3136,56), (3249,57), (3364,58), (3481,59), (3600,60), (3721,61), (3844,62), (3969,63), (4096,64), (4225,65), (4356,66), (4489,67), (4624,68), (4761,69), (4900,70), (5041,71), (5184,72), (5329,73), (5476,74), (56...

  forSortRDD.keyBy(x => x * x).sortByKey(true)
  //   res4: org.apache.spark.rdd.RDD[(Int, Int)] = ShuffledRDD[6] at sortByKey at <console>:26

  forSortRDD.keyBy(x => x * x).sortByKey(true).toDebugString
  //  res5: String = (8) ShuffledRDD[10] at sortByKey at <console>:26 []
  //    +-(8) MapPartitionsRDD[7] at keyBy at <console>:26 []
  //      |  ParallelCollectionRDD[0] at parallelize at <console>:24 []

  forSortRDD.keyBy(x => x * x).sortByKey(true).dependencies
  //  res6: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.ShuffleDependency@4dba554a)

  forSortRDD.keyBy(x => x * x).sortByKey(true).dependencies(0)
  //  res7: org.apache.spark.Dependency[_] = org.apache.spark.ShuffleDependency@347d91af


}
