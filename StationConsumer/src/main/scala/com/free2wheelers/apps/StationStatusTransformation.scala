package com.free2wheelers.apps

import java.time.Instant
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory.getLogger

import scala.util.parsing.json.JSON
import scala.util.{Failure, Try}
import ExceptionHandler._
import org.slf4j.Logger

object StationStatusTransformation {

  val log: Logger = getLogger(getClass)

  val sfAndNycToStationStatus: String => Seq[StationStatus] = raw_payload => {
    val json = JSON.parseFull(raw_payload)
    val payload = json.get.asInstanceOf[Map[String, Any]]("payload")
    extractSFAndNYCStationStatus(payload)
  }

  private def extractSFAndNYCStationStatus(payload: Any) = {

    val network: Any = payload.asInstanceOf[Map[String, Any]]("network")

    val stations: Any = network.asInstanceOf[Map[String, Any]]("stations")
    stations.asInstanceOf[Seq[Map[String, Any]]]
      .flatMap(x => {
        val station = Try(
          StationStatus(
            x("free_bikes").asInstanceOf[Double].toInt,
            x("empty_slots").asInstanceOf[Double].toInt,
            x("extra").asInstanceOf[Map[String, Any]]("renting").asInstanceOf[Double] == 1,
            x("extra").asInstanceOf[Map[String, Any]]("returning").asInstanceOf[Double] == 1,
            Instant.from(DateTimeFormatter.ISO_INSTANT.parse(x("timestamp").asInstanceOf[String])).getEpochSecond,
            x("id").asInstanceOf[String],
            x("name").asInstanceOf[String],
            x("latitude").asInstanceOf[Double],
            x("longitude").asInstanceOf[Double]
          ))
          handle(station).toOption
      })
  }

  def sfAndNycStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStatusFn: UserDefinedFunction = udf(sfAndNycToStationStatus)

    import spark.implicits._

    jsonDF.select(explode(toStatusFn(jsonDF("raw_payload"))) as "status")
      .select($"status.*")
  }

  val marseilleToStationStatus: String => Seq[StationStatus] = raw_payload => {
    val json = JSON.parseFull(raw_payload)
    val payload = json.get.asInstanceOf[Map[String, Any]]("payload")
    extractMarseilleStationStatus(payload)
  }

  private def extractMarseilleStationStatus(payload: Any) = {

    val network: Any = payload.asInstanceOf[Map[String, Any]]("network")

    val stations: Any = network.asInstanceOf[Map[String, Any]]("stations")

    stations.asInstanceOf[Seq[Map[String, Any]]]
      .flatMap(x => {
        val station = Try(
          StationStatus(
            x("free_bikes").asInstanceOf[Double].toInt,
            x("empty_slots").asInstanceOf[Double].toInt,
            is_renting = true,
            is_returning = true,
            Instant.from(DateTimeFormatter.ISO_INSTANT.parse(x("timestamp").asInstanceOf[String])).getEpochSecond,
            x("id").asInstanceOf[String],
            x("name").asInstanceOf[String],
            x("latitude").asInstanceOf[Double],
            x("longitude").asInstanceOf[Double]
          ))
        handle(station).toOption
      })
  }

  def marseilleStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStatusFn: UserDefinedFunction = udf(marseilleToStationStatus)

    import spark.implicits._
    jsonDF.select(explode(toStatusFn(jsonDF("raw_payload"))) as "status")
      .select($"status.*")
  }
}
