package com.free2wheelers.apps

import java.time.Instant
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory.getLogger

import scala.util.{Failure, Try}
import scala.util.parsing.json.JSON

object StationStatusTransformation {

  val log = getLogger(getClass)

  val sfToStationStatus: String => Seq[StationStatus] = raw_payload => {
    val json = JSON.parseFull(raw_payload)
    val payload = json.get.asInstanceOf[Map[String, Any]]("payload")
    extractSFStationStatus(payload)
  }

  private def extractSFStationStatus(payload: Any) = {

    val network: Any = payload.asInstanceOf[Map[String, Any]]("network")

    val stations: Any = network.asInstanceOf[Map[String, Any]]("stations")
    stations.asInstanceOf[Seq[Map[String, Any]]]
      .flatMap(x => {
        (Try(
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
          )) match {
          case f @ Failure(e: NoSuchElementException) =>
            log.error("Schema Error: ", e)
            f
          case f @ Failure(e: ClassCastException) =>
            log.error("Datatype mismatch Error: ", e)
            f
          case s => s
        }).toOption
      })
  }

  def sfStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStatusFn: UserDefinedFunction = udf(sfToStationStatus)

    import spark.implicits._

    jsonDF.select(explode(toStatusFn(jsonDF("raw_payload"))) as "status")
      .select($"status.*")
  }

  def nycStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    jsonDF.select(from_json($"raw_payload", ScalaReflection.schemaFor[StationStatus].dataType) as "status")
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
        (Try(
          StationStatus(
            x("free_bikes").asInstanceOf[Double].toInt,
            x("empty_slots").asInstanceOf[Double].toInt,
            true,
            true,
            Instant.from(DateTimeFormatter.ISO_INSTANT.parse(x("timestamp").asInstanceOf[String])).getEpochSecond,
            x("id").asInstanceOf[String],
            x("name").asInstanceOf[String],
            x("latitude").asInstanceOf[Double],
            x("longitude").asInstanceOf[Double]
          )) match {
          case f @ Failure(e: NoSuchElementException) =>
            log.error("Schema Error: ", e)
            f
          case f @ Failure(e: ClassCastException) =>
            log.error("Datatype mismatch Error: ", e)
            f
          case f @ Failure(e: DateTimeParseException) =>
            log.error("Timestamp parse Error: ", e)
            f
          case s => s
        }).toOption
      })
  }

  def marseilleStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStatusFn: UserDefinedFunction = udf(marseilleToStationStatus)

    import spark.implicits._
    jsonDF.select(explode(toStatusFn(jsonDF("raw_payload"))) as "status")
      .select($"status.*")
  }
}
