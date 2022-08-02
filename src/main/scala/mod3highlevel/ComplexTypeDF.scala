package mod3highlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object ComplexTypeDF extends App {

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()


  val employeeComplexDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/employee_complex/employee.txt")


  employeeComplexDF.printSchema()


  val employeeDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/employee/employees_headers.csv")

  employeeDF.printSchema()

  val structureSchema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("id", StringType)
    .add("gender", StringType)
    .add("salary", IntegerType)


  //  new ArrayType()
  //  new MapType()


}
