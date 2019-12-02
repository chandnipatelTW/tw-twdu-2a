package com.free2wheelers.apps

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object LatitudeLongitudeValidator{
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Unique Station Id Validator").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = "hdfs://emr-master.twdu-2a.training:8020/free2wheelers/stationMart/data"

    validate(spark, inputPath)
    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def validate(spark: SparkSession, inputPath: String): Boolean = {
    log.info("Reading text file from: " + inputPath)

    import spark.implicits._
    val emptyLatLongCount = spark
      .read
      .option("header", "true")
      .csv(inputPath)
      .filter("latitude is null")
      .filter("longitude is null")
      .count()

    log.info("Stations with empty latitude or longitude: " + emptyLatLongCount)
    emptyLatLongCount == 0
  }
}
