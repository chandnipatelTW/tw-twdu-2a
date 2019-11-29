package com.free2wheelers.apps

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{from_unixtime}

object StationUtils {

  implicit class StringDataset(val stations: Dataset[StationStatus]) {
    def formatLastUpdatedDate(dateFormat: String, spark: SparkSession) = {
      import spark.implicits._
      stations.withColumn("last_updated", from_unixtime($"last_updated", dateFormat))
    }
  }

}
