package ch4optimisation

import org.apache.spark.sql.SparkSession

object SharedVariables extends App {

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()


  /**
   * Exercise solve the problem with serialization
   *
   *
   *
   *
   *
   * */


  val sc = spark.sparkContext

  class NotSerializable(v: Int) {
    def getV = v
  }

  val ns = new NotSerializable(1)

  sc.parallelize(Array(1, 2, 3)).map { x =>
    // val ns = new NotSerializable(x)
    ns.getV
  }.collect()


  /** Theory */


  val accum = sc.longAccumulator("My Accumulator")

  val rdd4 = sc.parallelize(Array(1, 2, 3, 4), 4)

  rdd4.foreach(x => accum.add(x * 2))

  accum.value


}
