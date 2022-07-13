package ch3batch.highlevel


import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.DriverManager

class PostgreSQLContainerTest extends AnyFlatSpec with TestContainersForAll {

  val schema = "test"
  override type Containers = PostgreSQLContainer

  override def startContainers(): Containers = {
    val container = PostgreSQLContainer.Def("postgres").start()
    container
  }

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)
    container match {
      case _: PostgreSQLContainer => {
        Class.forName(container.driverClassName)

        val conn = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
        conn.createStatement.execute(s"create schema if not exists $schema;")
        conn.close()
      }
    }
  }

  override def beforeContainersStop (container: Containers): Unit = {
    super.beforeContainersStop(container)
    container match {
      case _: PostgreSQLContainer => {
        Class.forName(container.driverClassName)

        val conn = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)
        conn.createStatement.execute(s"drop schema if exists $schema cascade;")
        conn.close()
      }
    }
  }

  it should "PostgreSQL container start" in withContainers { case container: PostgreSQLContainer =>
    assert(container.jdbcUrl.nonEmpty)
    assert(container.username.nonEmpty)
    assert(container.password.nonEmpty)
  }

  it should "PostgreSQL container create table"  in withContainers { case container: PostgreSQLContainer =>
    Class.forName(container.driverClassName)
    val conn = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)

    conn.createStatement.execute(s"drop table if exists test;")
    conn.createStatement.execute(s"create table test(NAME text, PWD text);")

    val result = conn.createStatement.executeQuery(s"select * from pg_catalog.pg_tables where schemaname='$schema';")
    while (result.next()) {
      val table = result.getString(1)
      assert(table == "test")
    }
    conn.close()
  }

  it should "PostgreSQL container R/W records"  in withContainers { case container: PostgreSQLContainer =>
    Class.forName(container.driverClassName)
    val conn = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)

    conn.createStatement.execute(s"insert into test values ('test1', '123123');")
    conn.createStatement.execute(s"insert into test values ('test2', '321321');")

    val result = conn.createStatement.executeQuery(s"select count(*) from test;")
    while (result.next()) {
      val count = result.getInt(1)
      assert(count >= 1 )
    }
    conn.close()
  }



}
