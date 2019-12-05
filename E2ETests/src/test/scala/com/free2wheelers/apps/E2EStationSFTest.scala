package com.free2wheelers.apps

import java.sql.Timestamp.valueOf

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession
import org.scalatest._

class E2EStationSFTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("End to end test for SF station data") {
    Thread.sleep(18000)

    val spark = SparkSession.builder
      .appName("E2E Test")
      .master("local")
      .getOrCreate()

    val zookeeperConnectionString = "kafka-1.twdu-2a-qa.training:2181"

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
    zkClient.start()

    scenario("Transform SF station data frame") {

      val outputLocation = new String(zkClient.getData.watched.forPath("/free2wheelers/output/dataLocation"))
      val outputPath = outputLocation + "/year=2019/month=11/day=28/hour=5/minute=9"

      val result = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(outputPath).toDF()

      result.show(truncate = false)
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
      result.schema.fields(4).dataType.typeName should be("timestamp")
      result.schema.fields(5).name should be("station_id")
      result.schema.fields(5).dataType.typeName should be("string")
      result.schema.fields(6).name should be("name")
      result.schema.fields(6).dataType.typeName should be("string")
      result.schema.fields(7).name should be("latitude")
      result.schema.fields(7).dataType.typeName should be("double")
      result.schema.fields(8).name should be("longitude")
      result.schema.fields(8).dataType.typeName should be("double")

      val row1 = result.sort("last_updated").head()
      row1.get(0) should be(16)
      row1.get(1) should be(3)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(valueOf("2019-11-28 05:09:12"))
      row1.get(5) should be("73f8aa6ec3250de2a02cc0763f5a19eb")
      row1.get(6) should be("Mercado Way at Sierra Rd")
      row1.get(7) should be(37.37111641335901)
      row1.get(8) should be(-121.88133180141449)

      result.count() should be(3)
    }
  }
}
