package com.free2wheelers.apps

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object UniqueStationIdValidator{
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
    val duplicatesCount = spark
      .read
      .option("header", "true")
      .csv(inputPath)
      .groupBy($"station_id")
      .count
      .filter($"count">1)
      .count()

    log.info("Duplicates station Ids Count: " + duplicatesCount)
    duplicatesCount == 0
  }
}
