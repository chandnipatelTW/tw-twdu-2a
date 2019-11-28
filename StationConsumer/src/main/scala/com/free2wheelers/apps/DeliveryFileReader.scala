package com.free2wheelers.apps

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{SparkSession}

object DeliveryFileReader {
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("DeliveryFileValidation - Kales/Katie").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val inputPath = "hdfs://emr-master.twdu-2a.training:8020/free2wheelers/stationMart/data"

    run(spark, inputPath)
    log.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
  }

  def run(spark: SparkSession, inputPath: String): Unit = {
    log.info("Reading text file from: " + inputPath)

    import spark.implicits._
    val duplicatesCount = spark
      .read
      .csv(inputPath)
      .groupBy($"State").count.filter($"count">1).count()
    log.info("Duplicates Count: " + duplicatesCount)
  }

}
