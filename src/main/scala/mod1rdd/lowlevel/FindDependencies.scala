package mod1rdd.lowlevel

import org.apache.spark.RangeDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FindDependencies extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  /** Exercise
   * analyse functions:
   * .foldByKey()
   * .combineByKey()
   * .aggregateByKey()
   * .reduceByKey()
   * .countByKey()
   * .groupByKey()
   *
   * and find which type of RDD they produce MapPartitionsRDD
   * and find which type of RDD they produce ShuffleRDD
   *
   * and find out which type of Dependencies they use under the hood
   */


  /**
   * Picture
   * 1. abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T]
   *
   * 2. class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd)
   *
   * 3. class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
   *
   * 4. class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag]
   *
   *
   * */


  val sc = spark.sparkContext


  var parentRDD = sc.parallelize(0 to 100, 10)

  parentRDD.collect().sum

  parentRDD.getNumPartitions

  parentRDD.dependencies

  val childRDD = parentRDD.map(_ + 100500)

  childRDD.dependencies

  childRDD.dependencies(0).rdd

  childRDD.dependencies(0).rdd.collect()

  parentRDD.collect()

  parentRDD = sc.parallelize(0 to 50, 10)

  parentRDD.collect()

  /**
   * Could you please tell or write what should be do here?
   *
   * childRDD.dependencies(0).rdd.collect()
   *
   * */

  childRDD.dependencies(0).rdd.collect()


  childRDD.dependencies(0).rdd.dependencies
  //  res23: Seq[org.apache.spark.Dependency[_]] = List()

  childRDD.toDebugString


  import org.apache.spark.OneToOneDependency

  val one2one = childRDD.dependencies(0).asInstanceOf[OneToOneDependency[Int]]

  /**
   * RangeDependency
   *
   * */

  val firstRDD = sc.parallelize(1 to 100, 5)
  //  firstRDD: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[6] at parallelize at <console>:25

  val secondRDD = sc.parallelize(1 to 50, 10)
  //    secondRDD: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[7] at parallelize at <console>:25

  val unionRDD = firstRDD.union(secondRDD)
  //  unionRDD: org.apache.spark.rdd.RDD[Int] = UnionRDD[8] at union at <console>:28

  unionRDD.dependencies
  //  res32: Seq[org.apache.spark.Dependency[_]] = ArrayBuffer(org.apache.spark.RangeDependency@7b07c26d, org.apache.spark.RangeDependency@33406060)

  unionRDD.dependencies
  //  res32: Seq[org.apache.spark.Dependency[_]] = ArrayBuffer(org.apache.spark.RangeDependency@7b07c26d, org.apache.spark.RangeDependency@33406060)

  unionRDD.dependencies(0)
  //  res33: org.apache.spark.Dependency[_] = org.apache.spark.RangeDependency@7b07c26d

  unionRDD.dependencies(0).rdd
  //  res34: org.apache.spark.rdd.RDD[_] = ParallelCollectionRDD[6] at parallelize at <console>:25

  unionRDD.dependencies(0).rdd.getNumPartitions
  //    res35: Int = 5

  unionRDD.dependencies(1).rdd.getNumPartitions
  //    res36: Int = 10


  val rangeDependency = unionRDD.dependencies(0).asInstanceOf[RangeDependency[Int]]
  //  rangeDependency: org.apache.spark.RangeDependency[Int] = org.apache.spark.RangeDependency@7b07c26d

  rangeDependency
  //  res40: org.apache.spark.RangeDependency[Int] = org.apache.spark.RangeDependency@7b07c26d

  rangeDependency.rdd
  //  res41: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[6] at parallelize at <console>:25

  rangeDependency.getParents(0)
  //  res43: List[Int] = List(0)

  rangeDependency.getParents(4)
  //  res47: List[Int] = List(4)

  rangeDependency.getParents(5)
  //  res46: List[Int] = List()


  val rangeDependency2 = unionRDD.dependencies(1).asInstanceOf[RangeDependency[Int]]
  //  rangeDependency2: org.apache.spark.RangeDependency[Int] = org.apache.spark.RangeDependency@33406060

  rangeDependency2.getParents(2)
  //          res50: List[Int] = List()

  rangeDependency2.getParents(5)
  //          res52: List[Int] = List(0)

  rangeDependency2.getParents(6)
  //  res53: List[Int] = List(1)

  /**
   * ShuffleDependency
   *
   * */


  val sourceRDD = sc.parallelize(1 to 100, 5)
  val disRDD = sourceRDD.distinct
  //  disRDD: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[12] at distinct at <console>:27

  disRDD.dependencies
  //    res54: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.OneToOneDependency@76d54741)

  disRDD.dependencies(0).rdd
  //    res55: org.apache.spark.rdd.RDD[_] = ShuffledRDD[11] at distinct at <console>:27

  disRDD.dependencies(0).rdd.dependencies
  //      res56: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.ShuffleDependency@2256da1f)

  disRDD.dependencies(0).rdd.dependencies(0).rdd
  //      res57: org.apache.spark.rdd.RDD[_] = MapPartitionsRDD[10] at distinct at <console>:27

  disRDD.dependencies(0).rdd.dependencies(0).rdd.dependencies
  //        res58: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.OneToOneDependency@2326f67f)

  disRDD.dependencies(0).rdd.dependencies(0).rdd.dependencies(0)
  //        res59: org.apache.spark.Dependency[_] = org.apache.spark.OneToOneDependency@2326f67f

  disRDD.dependencies(0).rdd.dependencies(0).rdd.dependencies(0).rdd
  //        res60: org.apache.spark.rdd.RDD[_] = ParallelCollectionRDD[9] at parallelize at <console>:26

  disRDD.dependencies(0).rdd.dependencies(0).rdd.dependencies(0).rdd.collect
  //          collect   collectAsync

  disRDD.dependencies(0).rdd.dependencies(0).rdd.dependencies(0).rdd.collect
  //          res61: Array[_] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100)

  disRDD.dependencies(0).rdd.dependencies(0).rdd.collect
  //          res62: Array[_] = Array((1,null), (2,null), (3,null), (4,null), (5,null), (6,null), (7,null), (8,null), (9,null), (10,null), (11,null), (12,null), (13,null), (14,null), (15,null), (16,null), (17,null), (18,null), (19,null), (20,null), (21,null), (22,null), (23,null), (24,null), (25,null), (26,null), (27,null), (28,null), (29,null), (30,null), (31,null), (32,null), (33,null), (34,null), (35,null), (36,null), (37,null), (38,null), (39,null), (40,null), (41,null), (42,null), (43,null), (44,null), (45,null), (46,null), (47,null), (48,null), (49,null), (50,null), (51,null), (52,null), (53,null), (54,null), (55,null), (56,null), (57,null), (58,null), (59,null), (60,null), (61,null), (62,null), (63,null), (64,null), (65,null), (66,null), (67,null), (68,null), (69,null), (70,null), (71,null), (...

  disRDD.dependencies(0).rdd.collect
  //          collect   collectAsync

  disRDD.dependencies(0).rdd.collect
  //          res63: Array[_] = Array((100,null), (80,null), (30,null), (50,null), (40,null), (90,null), (70,null), (20,null), (60,null), (10,null), (41,null), (61,null), (81,null), (21,null), (71,null), (11,null), (51,null), (1,null), (91,null), (31,null), (52,null), (82,null), (22,null), (32,null), (92,null), (62,null), (42,null), (72,null), (12,null), (2,null), (13,null), (53,null), (73,null), (93,null), (33,null), (23,null), (63,null), (83,null), (3,null), (43,null), (84,null), (34,null), (4,null), (54,null), (14,null), (24,null), (64,null), (74,null), (44,null), (94,null), (15,null), (55,null), (25,null), (95,null), (65,null), (35,null), (75,null), (45,null), (85,null), (5,null), (96,null), (56,null), (76,null), (16,null), (66,null), (46,null), (36,null), (6,null), (86,null), (26,null), (47,null...


  val value: RDD[(Int, Int)] = sc.parallelize(List((1, 1), (2, 2)))

  val inputRDD = sc.parallelize(1 to 100, 5)
  //  inputRDD: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

  inputRDD.collect()
  //    res0: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100)

  inputRDD.partitions
  //    res3: Array[org.apache.spark.Partition] = Array(org.apache.spark.rdd.ParallelCollectionPartition@691, org.apache.spark.rdd.ParallelCollectionPartition@692, org.apache.spark.rdd.ParallelCollectionPartition@693, org.apache.spark.rdd.ParallelCollectionPartition@694, org.apache.spark.rdd.ParallelCollectionPartition@695)

  inputRDD.partitions(0)
  //    res4: org.apache.spark.Partition = org.apache.spark.rdd.ParallelCollectionPartition@691

  inputRDD.partitions(0).index
  //    res5: Int = 0

  /**
   * analyse functions:
   * .foldByKey()
   * .combineByKey()
   * .aggregateByKey()
   * .reduceByKey()
   * .countByKey()
   * .groupByKey()
   *
   * and find which type of RDD they produce MapPartitionsRDD
   * and find which type of RDD they produce ShuffleRDD
   *
   * and find out which type of Dependencies they use under the hood
   */

  //  val keyRDD = sc.parallelize(1 to 100, 5).map(x => (x, x + 10))

  val keyRddWithDuplicate = sc.parallelize(1 to 100).union(sc.parallelize(50 to 120))

  val fisrtRDD = sc.parallelize(1 to 100)
  //  fisrtRDD: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[3] at parallelize at <console>:29

  //  val secondRDD = sc.parallelize(50 to 150)
  //  secondRDD: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[4] at parallelize at <console>:29

  val uionRDD = fisrtRDD.union(secondRDD)
  //   uionRDD: org.apache.spark.rdd.RDD[Int] = UnionRDD[5] at union at <console>:32

  //  val keyRDD = uionRDD.map(x => (x, x + 10))
  //  keyRDD: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[6] at map at <console>:30

  //  val keyRDD = uionRDD.keyBy(x => x + 10)
  //  keyRDD: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[16] at keyBy at <console>:30

//  keyRDD.count
  //  res25: Long = 220

//  keyRDD.distinct.count
  //  res25: Long = 150

//  keyRDD.reduceByKey((v1, v2) => if (v1 > v2) v1 else v2).count
  //  res27: Long = 150

  // Example of first resolving
//  keyRDD.reduceByKey((v1, v2) => if (v1 > v2) v1 else v2).dependencies(0).rdd.dependencies(0)


}
