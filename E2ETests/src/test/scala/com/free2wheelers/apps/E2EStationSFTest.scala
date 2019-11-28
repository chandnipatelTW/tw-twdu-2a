package com.free2wheelers.apps

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession
import org.scalatest._

class E2EStationSFTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("End to end test for SF station data") {

    val spark = SparkSession.builder
      .appName("E2E Test")
      .master("local")
      .getOrCreate()

    val zookeeperConnectionString = "zookeeper:2181"

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
    zkClient.start()

    import spark.implicits._

    scenario("Transform SF station data frame") {
      val outputLocation = new String(zkClient.getData.watched.forPath("/free2wheelers/output/dataLocation"))

      println(outputLocation, "in e2e test ******")

      val result = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(outputLocation).toDF()

      result.show(truncate=false)
      result.printSchema()

      Given("The producer got data from mock server")

      Then("Transformed data should be available with expected schema")
      result.schema.fields(0).name should be("bikes_available")
      result.schema.fields(0).dataType.typeName should be("integer")
      result.schema.fields(1).name should be("docks_available")
      result.schema.fields(1).dataType.typeName should be("integer")
      result.schema.fields(2).name should be("is_renting")
      result.schema.fields(2).dataType.typeName should be("boolean")
      result.schema.fields(3).name should be("is_returning")
      result.schema.fields(3).dataType.typeName should be("boolean")
      result.schema.fields(4).name should be("last_updated")
      result.schema.fields(4).dataType.typeName should be("integer")
      result.schema.fields(5).name should be("station_id")
      result.schema.fields(5).dataType.typeName should be("string")
      result.schema.fields(6).name should be("name")
      result.schema.fields(6).dataType.typeName should be("string")
      result.schema.fields(7).name should be("latitude")
      result.schema.fields(7).dataType.typeName should be("double")
      result.schema.fields(8).name should be("longitude")
      result.schema.fields(8).dataType.typeName should be("double")

      val row1 = result.head()
      row1.get(0) should be(9)
      row1.get(1) should be(10)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1574917752)
      row1.get(5) should be("7a10659bcb8e88d97a98531df0dcd2be")
      row1.get(6) should be("Webster St at Clay St")
      row1.get(7) should be(37.79080303242391)
      row1.get(8) should be(-122.43259012699126)

      result.count() should be(3)
    }
  }
}
