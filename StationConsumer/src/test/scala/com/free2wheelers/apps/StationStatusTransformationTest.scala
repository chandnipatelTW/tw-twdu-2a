package com.free2wheelers.apps

import com.free2wheelers.apps.StationStatusTransformation.{sfAndNycStationStatusJson2DF, marseilleStationStatusJson2DF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalatest._

class StationStatusTransformationTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Transform SF station data frame") {

      val testStationData =
        """{
          |    "payload": {
          |        "network": {
          |            "stations": [ {
          |                "empty_slots": 11,
          |                "extra": {
          |                    "address": null, "last_updated": 1574751329, "renting": 1, "returning": 1, "uid": "345"
          |                }
          |                ,
          |                "free_bikes": 16,
          |                "id": "98cf498d2fa09046f98abeb6a9e902ff",
          |                "latitude": 37.766482696439496,
          |                "longitude": -122.39827930927277,
          |                "name": "Hubbell St at 16th St",
          |                "timestamp": "2019-11-26T09:26:54.819000Z"
          |            }
          |            ]
          |        }
          |    }
          |}""".stripMargin

      val schema = ScalaReflection.schemaFor[StationStatus].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(sfAndNycStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("long")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(16)
      row1.get(1) should be(11)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1574760414)
      row1.get(5) should be("98cf498d2fa09046f98abeb6a9e902ff")
      row1.get(6) should be("Hubbell St at 16th St")
      row1.get(7) should be(37.766482696439496)
      row1.get(8) should be(-122.39827930927277)
    }

    scenario("Transform SF data should ignore records with schema errors") {
      val testStationData =
        """{
          |    "payload": {
          |        "network": {
          |            "stations": [ {
          |                "empty_slots": 11,
          |                "extra": {
          |                    "address": null, "last_updated": 1574751329, "renting": 1, "returning": 1, "uid": "345"
          |                }
          |                ,
          |                "free_bikes": 16,
          |                "id": "98cf498d2fa09046f98abeb6a9e902ff",
          |                "latitude": 37.766482696439496,
          |                "longitude": -122.39827930927277,
          |                "name": "Hubbell St at 16th St",
          |                "timestamp": "2019-11-26T09:26:54.819000Z"
          |            },
          |            {
          |                "empty_slots": 10,
          |                "extra": {
          |                    "address": null, "last_updated": 1574751329, "renting": 1, "returning": 1, "uid": "345"
          |                }
          |                ,
          |                "available_bikes_error_field": 1,
          |                "id": "98cf498d2fa09046f98abeb6a9e902ff",
          |                "latitude": 37.766482696439496,
          |                "longitude": -122.39827930927277,
          |                "name": "Hubbell St at 16th St",
          |                "timestamp": "2019-11-26T09:26:54.819000Z"
          |            }
          |            ]
          |        }
          |    }
          |}""".stripMargin


      Given("Sample data for station_status with different schema")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(sfAndNycStationStatusJson2DF(_, spark))

      Then("Meaningful rows should be retrieved")

      resultDF1.count() should be(1)
      val row1 = resultDF1.head()
      row1.get(0) should be(16)
      row1.get(1) should be(11)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1574760414)
      row1.get(5) should be("98cf498d2fa09046f98abeb6a9e902ff")
      row1.get(6) should be("Hubbell St at 16th St")
      row1.get(7) should be(37.766482696439496)
      row1.get(8) should be(-122.39827930927277)
    }

    scenario("Transform SF data should ignore records with datatype errors") {
      val testStationData =
        """{
          |    "payload": {
          |        "network": {
          |            "stations": [ {
          |                "empty_slots": "11 empty slots",
          |                "extra": {
          |                    "address": null, "last_updated": 1574751329, "renting": 1, "returning": 1, "uid": "345"
          |                }
          |                ,
          |                "free_bikes": 16,
          |                "id": "98cf498d2fa09046f98abeb6a9e902ff",
          |                "latitude": 37.766482696439496,
          |                "longitude": -122.39827930927277,
          |                "name": "Hubbell St at 16th St",
          |                "timestamp": "2019-11-26T09:26:54.819000Z"
          |            },
          |            {
          |                "empty_slots": 10,
          |                "extra": {
          |                    "address": null, "last_updated": 1574751329, "renting": 1, "returning": 1, "uid": "345"
          |                }
          |                ,
          |                "available_bikes_schema_change": 1,
          |                "id": "98cf498d2fa09046f98abeb6a9e902ff",
          |                "latitude": 37.766482696439496,
          |                "longitude": -122.39827930927277,
          |                "name": "Hubbell St at 16th St",
          |                "timestamp": "2019-11-26T09:26:54.819000Z"
          |            },
          |            {
          |                "empty_slots": 10,
          |                "extra": {
          |                    "address": null, "last_updated": 1574751329, "renting": 1, "returning": 1, "uid": "345"
          |                }
          |                ,
          |                "free_bikes": 1,
          |                "id": "98cf498d2fa09046f98abeb6a9e902ff",
          |                "latitude": 37.766482696439496,
          |                "longitude": -122.39827930927277,
          |                "name": "Hubbell St at 16th St",
          |                "timestamp": "2019-11-26T09:26:54.819000Z"
          |            }
          |            ]
          |        }
          |    }
          |}""".stripMargin


      Given("Sample data for station_status with different schema")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(sfAndNycStationStatusJson2DF(_, spark))

      Then("Meaningful rows should be retrieved")

      resultDF1.count() should be(1)
      val row1 = resultDF1.head()
      row1.get(0) should be(1)
      row1.get(1) should be(10)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1574760414)
      row1.get(5) should be("98cf498d2fa09046f98abeb6a9e902ff")
      row1.get(6) should be("Hubbell St at 16th St")
      row1.get(7) should be(37.766482696439496)
      row1.get(8) should be(-122.39827930927277)
    }

    scenario("Transform Marseille station data frame") {

      val testStationData =
        """{
               "payload": {
                "network": {
                    "stations": [ {
                        "empty_slots": 12,
                        "extra": {
                            "address": "391 MICHELET - 391 BOULEVARD MICHELET", "banking": true, "bonus": false, "last_update": 1574764228000, "slots": 19, "status": "OPEN", "uid": 8149
                        }
                        ,
                        "free_bikes": 7,
                        "id": "686e48654a218c70daf950a4e893e5b0",
                        "latitude": 43.25402727813068,
                        "longitude": 5.401873594694653,
                        "name": "8149-391 MICHELET",
                        "timestamp": "2019-11-26T10:33:52.302000Z"
                    }
                    ]
                }
            }
          |}""".stripMargin

      val schema = ScalaReflection.schemaFor[StationStatus].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")

      When("Transformations are applied")
      val resultDF1 = testDF1.transform(marseilleStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("long")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(7)
      row1.get(1) should be(12)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1574764432)
      row1.get(5) should be("686e48654a218c70daf950a4e893e5b0")
      row1.get(6) should be("8149-391 MICHELET")
      row1.get(7) should be(43.25402727813068)
      row1.get(8) should be(5.401873594694653)
    }

    scenario("Transform Marseille data should ignore records with schema errors") {

      val testStationData =
        """{
               "payload": {
                "network": {
                    "stations": [ {
                        "empty_slots_error": 1,
                        "extra": {
                            "address": "391 MICHELET - 391 BOULEVARD MICHELET", "banking": true, "bonus": false, "last_update": 1574764228000, "slots": 19, "status": "OPEN", "uid": 8149
                        }
                        ,
                        "free_bikes": 3,
                        "id": "686e48654a218c70daf950a4e893e5b0",
                        "latitude": 43.25402727813068,
                        "longitude": 5.401873594694653,
                        "name": "8149-391 MICHELET",
                        "timestamp": "2019-11-26T10:33:52.302000Z"
                    },{
                        "empty_slots": 12,
                        "extra": {
                            "address": "391 MICHELET - 391 BOULEVARD MICHELET", "banking": true, "bonus": false, "last_update": 1574764228000, "slots": 19, "status": "OPEN", "uid": 8149
                        }
                        ,
                        "free_bikes": 7,
                        "id": "686e48654a218c70daf950a4e893e5b0",
                        "latitude": 43.25402727813068,
                        "longitude": 5.401873594694653,
                        "name": "8149-391 MICHELET",
                        "timestamp": "2019-11-26T10:33:52.302000Z"
                    }
                    ]
                }
            }
          |}""".stripMargin

      val schema = ScalaReflection.schemaFor[StationStatus].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")

      When("Transformations are applied")
      val resultDF1 = testDF1.transform(marseilleStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("long")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(7)
      row1.get(1) should be(12)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1574764432)
      row1.get(5) should be("686e48654a218c70daf950a4e893e5b0")
      row1.get(6) should be("8149-391 MICHELET")
      row1.get(7) should be(43.25402727813068)
      row1.get(8) should be(5.401873594694653)
    }

    scenario("Transform Marseille data should ignore records with datatype errors") {

      val testStationData =
        """{
               "payload": {
                "network": {
                    "stations": [ {
                        "empty_slots_error": 1,
                        "extra": {
                            "address": "391 MICHELET - 391 BOULEVARD MICHELET", "banking": true, "bonus": false, "last_update": 1574764228000, "slots": 19, "status": "OPEN", "uid": 8149
                        }
                        ,
                        "free_bikes": 3,
                        "id": "686e48654a218c70daf950a4e893e5b0",
                        "latitude": 43.25402727813068,
                        "longitude": 5.401873594694653,
                        "name": "8149-391 MICHELET",
                        "timestamp": "2019-11-26T10:33:52.302000Z"
                    },{
                        "empty_slots": 4,
                        "extra": {
                            "address": "391 MICHELET - 391 BOULEVARD MICHELET", "banking": true, "bonus": false, "last_update": 1574764228000, "slots": 19, "status": "OPEN", "uid": 8149
                        }
                        ,
                        "free_bikes": 5,
                        "id": "686e48654a218c70daf950a4e893e5b0",
                        "latitude": 43.25402727813068,
                        "longitude": 5.401873594694653,
                        "name": "8149-391 MICHELET",
                        "timestamp": "some time"
                    },{
                        "empty_slots": 1,
                        "extra": {
                            "address": "391 MICHELET - 391 BOULEVARD MICHELET", "banking": true, "bonus": false, "last_update": 1574764228000, "slots": 19, "status": "OPEN", "uid": 8149
                        }
                        ,
                        "free_bikes": "free bikes are 1",
                        "id": "686e48654a218c70daf950a4e893e5b0",
                        "latitude": 43.25402727813068,
                        "longitude": 5.401873594694653,
                        "name": "8149-391 MICHELET",
                        "timestamp": "some time"
                    },{
                        "empty_slots": 12,
                        "extra": {
                            "address": "391 MICHELET - 391 BOULEVARD MICHELET", "banking": true, "bonus": false, "last_update": 1574764228000, "slots": 19, "status": "OPEN", "uid": 8149
                        }
                        ,
                        "free_bikes": 7,
                        "id": "686e48654a218c70daf950a4e893e5b0",
                        "latitude": 43.25402727813068,
                        "longitude": 5.401873594694653,
                        "name": "8149-391 MICHELET",
                        "timestamp": "2019-11-26T10:33:52.302000Z"
                    }
                    ]
                }
            }
          |}""".stripMargin

      val schema = ScalaReflection.schemaFor[StationStatus].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")

      When("Transformations are applied")
      val resultDF1 = testDF1.transform(marseilleStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("long")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(7)
      row1.get(1) should be(12)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1574764432)
      row1.get(5) should be("686e48654a218c70daf950a4e893e5b0")
      row1.get(6) should be("8149-391 MICHELET")
      row1.get(7) should be(43.25402727813068)
      row1.get(8) should be(5.401873594694653)
    }
  }
}
