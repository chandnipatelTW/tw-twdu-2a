package com.free2wheelers.apps

import com.free2wheelers.apps.ExceptionHandler._
import org.scalatest._

import scala.util.Try

class ExceptionHandlerTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Handle exceptions") {
    scenario("Handle success case") {
      val station = Try(StationStatus(
        10,
        5,
        is_renting = true,
        is_returning = true,
        123456789,
        "station id",
        "some name",
        22.234,
        -123.123
      ))

      val result = handle(station)

      result.isSuccess shouldBe true
      result.isFailure shouldBe false
    }

    scenario("Handle failure case") {
      val station = Try(StationStatus(
        "10".asInstanceOf[Double].toInt,
        5,
        is_renting = true,
        is_returning = true,
        123456789,
        "station id",
        "some name",
        22.234,
        -123.123
      ))

      val result = handle(station)

      result.isSuccess shouldBe false
      result.isFailure shouldBe true
    }
  }
}
