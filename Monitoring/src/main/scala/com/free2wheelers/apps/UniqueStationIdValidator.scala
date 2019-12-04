package com.free2wheelers.apps

import com.free2wheelers.apps.LatitudeLongitudeValidator.defaultOutput
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object UniqueStationIdValidator{
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.INFO)
  val defaultInput = "hdfs://emr-master.twdu-2a.training:8020/user/hadoop/nisha/data/minute=37"
  val defaultOutput = "hdfs://ip-10-0-3-209.ap-southeast-1.compute.internal:8020/free2wheelers/monitoring/UniqueStationIdValidator/output.txt"

  def main(args: Array[String]): Unit = {
    val inputPath = if(!args.isEmpty) args(0) else defaultInput

    val spark = SparkSession.builder.appName("Unique Station Id Validator").getOrCreate()
    log.info("Application Initialized: " + spark.sparkContext.appName)

    val duplicatesCount = validate(spark, inputPath)

    log.info("Duplicates station Ids Count: " + duplicatesCount)
    HDFSFileWriter.write(defaultOutput,duplicatesCount.toString)
    log.info("Application Done: " + spark.sparkContext.appName)

    spark.stop()
  }

  def validate(spark: SparkSession, inputPath: String): Long = {
    log.info("Reading text file from: " + inputPath)

    import spark.implicits._
    spark
      .read
      .option("header", "true")
      .csv(inputPath)
      .groupBy($"station_id")
      .count
      .filter($"count">1)
      .count()
  }
}
