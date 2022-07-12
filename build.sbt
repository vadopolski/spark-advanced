ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings {
    name := "spark-advanced"
  }

val sparkVersion = "3.3.0"
val scalaTestVersion = "3.2.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  // logging
//  "log4j" % "log4j" % "1.2.17",
//  "org.slf4j" % "slf4j-log4j12" % "1.7.30",
//
//  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
//  "org.scalacheck" %% "scalacheck" % "1.14.3" ,
//  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0",
//
//  "org.scalikejdbc"         %% "scalikejdbc"                          % "3.5.0"   % Test,
//  "org.scalikejdbc"         %% "scalikejdbc-test"                     % "3.5.0"   % Test,
//  "com.dimafeng"            %% "testcontainers-scala-postgresql"      % "0.38.7"  % Test,
//  "com.dimafeng"            %% "testcontainers-scala-scalatest"       % "0.38.7"  % Test,
//  "org.flywaydb"            % "flyway-core"                         % "7.3.2",
//  "org.postgresql"          % "postgresql"                            % "42.2.2" ,
)