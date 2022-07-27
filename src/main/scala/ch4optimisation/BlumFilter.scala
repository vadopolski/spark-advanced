package ch4optimisation

import org.apache.spark.sql.{DataFrame, SparkSession}

object BlumFilter extends App {
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)

  import org.apache.spark.sql._

  val spark = {
    SparkSession.builder()
      .config("spark.sql.autoBroadcastJoinThreshold", 0)
      .master("local[*]")
      .getOrCreate()
  }

  def sc = spark.sparkContext // Разница между def, val, lazy val
  import spark.implicits._


  /**
  * Solve problem with a skew join problem
  *
  * */


  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/crimes/crime.csv")

  crimeFacts.show()

  val offenseCodes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/crimes/offense_codes.csv")

  offenseCodes.show(100, false)


  /**
   * Sort merge join
   *
   * */
  val robberyStats = crimeFacts
    .join(offenseCodes, $"CODE" === $"OFFENSE_CODE")
    .filter($"NAME".startsWith("ROBBERY"))
    .groupBy($"NAME")
    .count()
    .orderBy($"count".desc)

  robberyStats.show(false)
  robberyStats.explain(true)


  /**
   * Broadcast example
   *
   * */

  import org.apache.spark.sql.functions.broadcast

  val offenseCodesBroadcast = broadcast(offenseCodes)

  val robberyStatsWithBroadcast = crimeFacts
    .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")
    .filter($"NAME".startsWith("ROBBERY"))
    .groupBy($"NAME")
    .count()
    .orderBy($"count".desc)

  robberyStatsWithBroadcast.show(false)


  /**
   * Start blum filter example
   * */


  val codesTotal = offenseCodes
    .filter('NAME.startsWith("ROBBERY"))
    .select('CODE)
    .distinct
    .count()

  val bf = offenseCodes
    .filter('NAME.startsWith("ROBBERY"))
    .stat.bloomFilter('CODE, codesTotal, 0.5)

  import org.apache.spark.sql.functions.udf

  def mc(s: Int): Boolean = {
    bf.mightContain(s)
  }

  val mightContain = udf(mc _)

  // offenseCodes.filter('NAME.startsWith("ROBBERY")).show(1000)
  // offenseCodes.filter(mightContain('CODE)).show(1000)

  val robberyStatsWithBf = crimeFacts
    .filter(mightContain('OFFENSE_CODE))
    .join(offenseCodes, 'CODE === 'OFFENSE_CODE)
    .filter('NAME.startsWith("ROBBERY"))
    .groupBy('NAME)
    .count()
    .orderBy('count.desc)

  // robberyStatsWithBf.explain(true)
  robberyStatsWithBf.show(true)


  /**
   * End example of blum filter
   *
   * */










  val cachedCrimeFacts = crimeFacts.select("INCIDENT_NUMBER").cache()

  cachedCrimeFacts.count()
  cachedCrimeFacts.count()

  cachedCrimeFacts.unpersist()

  robberyStats.explain(true)

  crimeFacts.show

  crimeFacts.select($"year", $"month").distinct.count

  crimeFacts.show

  crimeFacts.selectExpr("DISTRICT", "percentile_approx(Lat, 0.5) over (partition by DISTRICT) as medianLat").show

  crimeFacts.createOrReplaceTempView("crimeFacts")
  spark.sql("select DISTRICT, percentile_approx(Lat, 0.5) as medianLat from crimeFacts group by DISTRICT").show


  val colList = crimeFacts.columns

  val req = s"""select ${colList.mkString(", ")} from target"""

  val colsForTarget = List("id", "new_id") ++ colList

//  crimeFacts.select(colsForTarget: _*)
}
