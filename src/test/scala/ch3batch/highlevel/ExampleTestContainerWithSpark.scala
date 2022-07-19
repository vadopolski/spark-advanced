package ch3batch.highlevel

import com.dimafeng.testcontainers.{Container, ForAllTestContainer, PostgreSQLContainer}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec



class ExampleTestContainerWithSpark  extends AnyFlatSpec with ForAllTestContainer  {
  override val container: PostgreSQLContainer = PostgreSQLContainer()

  it should "PostgreSQL container start" in {
    assert(container.jdbcUrl.nonEmpty)
    assert(container.username.nonEmpty)
    assert(container.password.nonEmpty)
  }






}
