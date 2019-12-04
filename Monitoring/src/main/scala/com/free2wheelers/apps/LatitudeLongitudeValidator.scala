package com.free2wheelers.apps

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object LatitudeLongitudeValidator {
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  val defaultInput = "hdfs://emr-master.twdu-2a.training:8020/free2wheelers/stationMart/data"
  val defaultOutput = "hdfs://ip-10-0-3-209.ap-southeast-1.compute.internal:8020/free2wheelers/monitoring/LatitudeLongitudeValidator/output.txt"

  def main(args: Array[String]): Unit = {
    val inputPath = if (!args.isEmpty) args(0) else defaultInput

    val spark = SparkSession.builder.appName("Unique Station Id Validator").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val emptyLatLongCount = validate(spark, inputPath)

    log.info("Stations with empty latitude or longitude: " + emptyLatLongCount)
    HDFSFileWriter.write(defaultOutput, emptyLatLongCount.toString)

    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def validate(spark: SparkSession, inputPath: String): Long = {
    log.info("Reading text file from: " + inputPath)

    spark
      .read
      .option("header", "true")
      .csv(inputPath)
      .filter("latitude is null")
      .filter("longitude is null")
      .count()
  }
}
