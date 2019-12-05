package com.free2wheelers.apps

import com.free2wheelers.apps.StationStatusTransformation._
import com.free2wheelers.apps.StationUtils._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory.getLogger

object StationApp {

  def main(args: Array[String]): Unit = {

    val log = getLogger(this.getClass)
    log.info("Initializing Station Consumer")

    val zookeeperConnectionString = if (args.isEmpty) "zookeeper:2181" else args(0)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()

    val stationKafkaBrokers = new String(zkClient.getData.forPath("/free2wheelers/stationStatus/kafkaBrokers"))

    val nycStationTopic = new String(zkClient.getData.watched.forPath("/free2wheelers/stationDataNYC/topic"))
    val sfStationTopic = new String(zkClient.getData.watched.forPath("/free2wheelers/stationDataSF/topic"))
    val marseilleStationTopic = new String(zkClient.getData.watched.forPath("/free2wheelers/stationDataMarseille/topic"))

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath("/free2wheelers/output/checkpointLocation"))

    val outputLocation = new String(
      zkClient.getData.watched.forPath("/free2wheelers/output/dataLocation"))

    val spark = SparkSession.builder
      .appName("StationConsumer")
      .getOrCreate()

    import spark.implicits._

    val nycStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", nycStationTopic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(sfAndNycStationStatusJson2DF(_, spark))

    val sfStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", sfStationTopic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(sfAndNycStationStatusJson2DF(_, spark))

    val marseilleStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", marseilleStationTopic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(marseilleStationStatusJson2DF(_, spark))

    val dateFormat = "yyyy-MM-dd'T'hh:mm:ss"

    nycStationDF
      .union(sfStationDF)
      .union(marseilleStationDF)
      .as[StationStatus]
      .groupByKey(r => r.station_id)
      .reduceGroups((r1, r2) => if (r1.last_updated > r2.last_updated) r1 else r2)
      .map(_._2)
      .formatLastUpdatedDate(dateFormat, spark)
      .withColumn("year", year($"last_updated"))
      .withColumn("month", month($"last_updated"))
      .withColumn("day", dayofmonth($"last_updated"))
      .withColumn("hour", hour($"last_updated"))
      .withColumn("minute", minute($"last_updated"))
      .writeStream
      .partitionBy("year", "month", "day", "hour", "minute")
      .outputMode("update")
      .format("overwriteCSV")
      .option("header", true)
      .option("truncate", false)
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputLocation)
      .start()
      .awaitTermination()

  }
}
