package com.free2wheelers.apps

import java.time.LocalDateTime

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}

object DeliveryFileReader {
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("DeliveryFileValidation - Kales/Katie").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = "hdfs://free2wheelers/stationMart/data"

    run(spark, inputPath)
    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String): Unit = {
    log.info("Reading text file from: " + inputPath)

    import spark.implicits._
    spark
      .read
      .text(inputPath)
      .as[String]
      .collect()
//      .count()
  }
}
