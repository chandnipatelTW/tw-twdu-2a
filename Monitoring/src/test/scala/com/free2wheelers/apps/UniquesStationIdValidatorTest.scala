package com.free2wheelers.apps

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest._

class UniqueStationValidatorTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Unique Station Validator") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

    scenario("Validates for unique station ids") {
      Given("station Mart data file with unique station Ids")
      val inputPath = "src/test/resources/StationMartData/Valid.csv"

      When("validator runs")
      val duplicatesCount = UniqueStationIdValidator.validate(spark, inputPath)

      Then("returns 0")
      duplicatesCount shouldBe 0
    }

    scenario("InValidates for duplicate station ids") {
      Given("station Mart data file with unique station Ids")
      val inputPath = "src/test/resources/StationMartData/Invalid.csv"

      When("validator runs")
      val duplicatesCount = UniqueStationIdValidator.validate(spark, inputPath)

      Then("returns 1")
      duplicatesCount shouldBe 1
    }

  }
}
