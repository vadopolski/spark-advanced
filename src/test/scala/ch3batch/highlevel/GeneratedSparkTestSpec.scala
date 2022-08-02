package ch3batch.highlevel

import mod3highlevel.DemoDataFrame.processTaxiData
import mod3highlevel.model.TaxiZone
import org.apache.spark.sql.test.SharedSparkSession
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks.forAll


class GeneratedSparkTestSpec extends SharedSparkSession {

  import testImplicits._
  import mod3highlevel.model._

  test("Thank you Captain") {
    forAll { l: List[String] =>
      l.reverse.reverse shouldBe l
      println(l)
    }
  }

  test("Bag test") {
    import DataGenerator._
    implicit val taxiZone: Arbitrary[TaxiZone]  = Arbitrary(genTaxiZone)
    implicit val taxiFacts: Arbitrary[TaxiRide] = Arbitrary(genTaxiFacts)

    forAll { (zones: Seq[TaxiZone], facts: Seq[TaxiRide]) =>
      val zonesDF = zones.toDF()
      val factsDF = facts.toDF()

      val actualResult = processTaxiData(factsDF, zonesDF)

      actualResult.show()

      actualResult.foreach { r =>
        val min = r.get(2).asInstanceOf[Double]
        val avg = r.get(3).asInstanceOf[Double]
        val max = r.get(4).asInstanceOf[Double]

        (min <= avg) shouldBe true
        (avg <= max) shouldBe true

        println(s"test OK")
      }
    }
  }


}
