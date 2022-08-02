package ch3batch.highlevel

import mod3highlevel.DemoDataFrame.processTaxiData
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession


class SparkTestSpec extends SharedSparkSession {
  import mod3highlevel.ReadWriteUtils._
  import testImplicits._

  test("join - join using") {
    val df  = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str") // 1 "1", 2 "2" , 3 "3"
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")  // 1 "2", 2 "3" , 3 "4"

    val actualFrameResult = df.join(df2, "int")


    checkAnswer(actualFrameResult, Row(1, "1", "2") :: Row(2, "2", "3") :: Row(3, "3", "4") :: Nil)
  }

  test("join - processTaxiData") {
    val taxiZonesDF2 = readCSV("src/test/resources/taxi_zones.csv")
    val taxiDF2      = readParquet("src/test/resources/yellow_taxi_jan_25_2018")

    val actualDistribution = processTaxiData(taxiZonesDF2, taxiDF2)

    checkAnswer(
      actualDistribution,
      Row("Manhattan", 296529, 0.0, 2.18, 37.92) ::
        Row("Queens", 13822, 0.0, 8.71, 51.6) ::
        Row("Brooklyn", 12673, 0.0, 6.89, 44.8) ::
        Row("Unknown", 6714, 0.0, 3.45, 66.0) ::
        Row("Bronx", 1590, 0.0, 9.05, 31.18) ::
        Row("EWR", 508, 0.0, 16.97, 45.98) ::
        Row("Staten Island", 67, 0.0, 19.49, 33.78) :: Nil
    )
  }
}