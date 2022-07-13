package ch3batch.highlevel

import com.dimafeng.testcontainers.{Container, ForAllTestContainer, PostgreSQLContainer}
import org.apache.spark.sql.test.SharedSparkSession



class ExampleTestContainerWithSpark extends SharedSparkSession with ForAllTestContainer  {
  override val container: PostgreSQLContainer = PostgreSQLContainer()







}
