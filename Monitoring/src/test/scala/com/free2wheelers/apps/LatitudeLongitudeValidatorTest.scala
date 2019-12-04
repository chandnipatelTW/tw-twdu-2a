package com.free2wheelers.apps

import org.apache.spark.sql.SparkSession
import org.scalatest._

class LatitudeLongitudeValidatorTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Latitude Longitude Validator") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

    scenario("Validates if each station has latitude and longitude") {
      Given("station Mart data file with latitude and longitude for each station")
      val inputPath = "src/test/resources/StationMartData/Valid.csv"

      When("validator runs")
      val duplicatesCount = LatitudeLongitudeValidator.validate(spark, inputPath)

      Then("returns 0")
      duplicatesCount shouldBe 0
    }

    scenario("InValidates for duplicate station ids") {
      Given("station Mart data file with unique station Ids")
      val inputPath = "src/test/resources/StationMartData/Invalid.csv"

      When("validator runs")
      val duplicatesCount = LatitudeLongitudeValidator.validate(spark, inputPath)

      Then("returns 2")
      duplicatesCount shouldBe 2
    }

  }
}
