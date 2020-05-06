package com.spark.project

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Driver {

  /**
   * Main method to be called from `spark-submit`.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val distributedSparkSession = SparkSession.builder().appName("Test").getOrCreate()

    val approval = readData(distributedSparkSession, "data/approval-topline.csv")
    approval.write.mode(SaveMode.Overwrite).parquet("/target/testing-approval-data")

    val us = readData(distributedSparkSession, "data/us.csv")
    us.write.mode(SaveMode.Overwrite).parquet("/target/testing-us-data")

    val usStates = readData(distributedSparkSession, "data/us-states.csv")
    usStates.write.mode(SaveMode.Overwrite).parquet("/target/testing-us-states-data")

    val usCounties = readData(distributedSparkSession, "data/us-counties.csv")
    usCounties.write.mode(SaveMode.Overwrite).parquet("/target/testing-us-counties-data")

  }

  /**
   * Reads data from a given path.
   *
   * @param sparkSession
   * @param path
   * @return
   */
  def readData(sparkSession: SparkSession, path: String): DataFrame = {
    val csvReadOptions = Map("inferSchema" -> true.toString, "header" -> true.toString)
    val data = sparkSession.read.options(csvReadOptions).csv(path)
    data
  }
}