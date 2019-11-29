package com.free2wheelers.apps

import org.apache.spark.sql.{SparkSession, Dataset}
import org.scalatest._

import StationUtils._

class StationUtilsTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Station Data Beautifier") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()


    scenario("Format last updated date to ISO format") {
      import spark.implicits._
      Given("Sample data for stations and date format")
      val station1 = StationStatus(1, 1, true, true, 1575015807, "Station1", "Station One", 55.1, 43.2)
      val station2 = StationStatus(4, 1, true, true, 1575016922, "Station2", "Station Two", 60.45, 46.76)
      val stations: Dataset[StationStatus] = Seq(station1, station2).toDS()
      val dateFormat = "yyyy-MM-dd'T'hh:mm:ss"

      When("format is applied")
      val stationsWithFormattedDate = stations formatLastUpdatedDate(dateFormat, spark)

      Then("Last updated date is formatted to the given format")
      val expected = Seq("2019-11-29T08:23:27", "2019-11-29T08:42:02").toDF().collect

      stationsWithFormattedDate.select($"last_updated").collect should contain theSameElementsAs expected
    }

  }
}
