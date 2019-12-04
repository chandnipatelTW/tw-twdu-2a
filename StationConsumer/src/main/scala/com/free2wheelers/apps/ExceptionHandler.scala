package com.free2wheelers.apps

import java.time.format.DateTimeParseException

import com.free2wheelers.apps.StationStatusTransformation.log

import scala.util.{Failure, Try}

object ExceptionHandler {

  def handle(tryExtractor: Try[StationStatus]): Try[StationStatus] = {
    tryExtractor match {
      case f@Failure(e: NoSuchElementException) =>
        log.error("Schema Error: ", e)
        f
      case f@Failure(e: ClassCastException) =>
        log.error("Datatype mismatch Error: ", e)
        f
      case f@Failure(e: DateTimeParseException) =>
        log.error("Timestamp parse Error: ", e)
        f
      case s => s
    }
  }
}
