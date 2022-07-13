package ch3batch.highlevel

import org.scalacheck.Gen
import org.scalacheck.Gen.posNum


object DataGenerator {
  import ch3batch.model._

  implicit val genString: Gen[String] =
    Gen.listOfN[Char](14, Gen.alphaChar).map(_.mkString).filter(x => x.nonEmpty && x != null)

  implicit val genPositiveInt: Gen[Int] = Gen.oneOf(1 to 5)
  implicit val genLocationId: Gen[Int] = Gen.oneOf(1 to 5)
  implicit val genBorough: Gen[String] = Gen.oneOf("Queens", "Brooklyn", "Unknown", "Bronx", "EWR", "Staten Island")

  implicit val genTaxiZone: Gen[TaxiZone] = for {
    locationID   <- genLocationId
    borough      <- genBorough
    zone         <- genString
    service_zone <- genString
  } yield TaxiZone(
    locationID,
    borough,
    zone,
    service_zone
  )

  implicit val genTaxiFacts: Gen[TaxiRide] = for {
    vendorID              <- posNum[Int]
    tpep_pickup_datetime  <- genString
    tpep_dropoff_datetime <- genString
    passenger_count       <- genPositiveInt
    trip_distance         <- posNum[Double]
    ratecodeID            <- posNum[Int]
    store_and_fwd_flag    <- genString
    pULocationID          <- genLocationId
    dOLocationID          <- genLocationId
    payment_type          <- posNum[Int]
    fare_amount           <- posNum[Double]
    extra                 <- posNum[Double]
    mta_tax               <- posNum[Double]
    tip_amount            <- posNum[Double]
    tolls_amount          <- posNum[Double]
    improvement_surcharge <- posNum[Double]
    total_amount          <- posNum[Double]
  } yield TaxiRide(
    vendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeID,
    store_and_fwd_flag,
    pULocationID,
    dOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount
  )

}
