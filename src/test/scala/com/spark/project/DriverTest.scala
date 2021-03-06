package com.spark.project

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DriverTest extends FunSuite {

  val spark = SparkSession.builder().appName("Project").master("local[*]").getOrCreate()

  test("Read approval data") {
    val approvalData = Driver.readData(spark, "data/approval-topline.csv")
    assert(approvalData.count() === 3600)
  }

  test("Read us data") {
    val usData = Driver.readData(spark, "data/us.csv")
    assert(usData.count() === 96)
  }

  test("Read us states data") {
    val usStatesData = Driver.readData(spark, "data/us-states.csv")
    assert(usStatesData.count() === 2984)
  }

  test("Read us counties data") {
    val usCountiesData = Driver.readData(spark, "data/us-counties.csv")
    assert(usCountiesData.count() === 89772)
  }
}
