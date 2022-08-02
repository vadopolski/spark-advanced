package ch3batch.highlevel


import mod3highlevel.PostgresWrite._
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.matchers.should.Matchers

import java.sql.DriverManager
import java.util.Properties

class DistributionByDistanceInfoTest extends SharedSparkSession with ForAllTestContainer with Matchers {

  override val container: PostgreSQLContainer = PostgreSQLContainer()
  val parquetPath = "src/test/resources/yellow_taxi_jan_25_2018"

  def getCntrConnProps(): Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", container.username)
    connectionProperties.put("password", container.password)
    connectionProperties.put("driver", "org.postgresql.Driver")
    connectionProperties.put("url", container.jdbcUrl)

    connectionProperties
  }


  test("DistributionByDistance write to Postgres test.") {
    val properties = getCntrConnProps()

    val taxiDF = loadParquet2DF( parquetPath )
    val distanceDistribution = taxiPickupsByDistanceRange( taxiDF )

    writeDF2Postgres( distanceDistribution, "testTbl", properties)

    val conn = DriverManager.getConnection( container.jdbcUrl, properties)
    try {
      val stmt = conn.prepareStatement("select count(*) cnt from testTbl")
      val rs = stmt.executeQuery()
      rs.next()
      val cnt = rs.getInt( 1 )

      cnt shouldBe 11
    }
    finally {
      conn.close()
    }
  }

}
