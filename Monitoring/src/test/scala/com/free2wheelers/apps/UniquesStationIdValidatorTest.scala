package com.free2wheelers.apps

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest._

class UniqueStationValidatorTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Unique Station Validator") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

    scenario("Validates for unique station ids") {
      Given("station Mart data file with unique station Ids")
      val inputPath = "src/test/resources/StationMartData/UniqueStationIds.csv"

      When("validator runs")
      val duplicatesCount = UniqueStationIdValidator.validate(spark, inputPath)

      Then("returns true")
      duplicatesCount shouldBe true
    }

    scenario("InValidates for duplicate station ids") {
      Given("station Mart data file with unique station Ids")
      val inputPath = "src/test/resources/StationMartData/DuplicateStationIds.csv"

      When("validator runs")
      val duplicatesCount = UniqueStationIdValidator.validate(spark, inputPath)

      Then("returns true")
      duplicatesCount shouldBe false
    }

  }
}
