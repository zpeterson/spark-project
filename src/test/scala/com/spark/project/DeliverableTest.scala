package com.spark.project

import com.spark.project.dto.{Approval, Us, UsCounties, UsStates}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class DeliverableTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val spark = SparkSession.builder().appName("Project").master("local[*]").getOrCreate()

  /**
    * Let Spark infer the data types.
    */
  val csvReadOptions = Map("inferSchema" -> true.toString, "header" -> true.toString)

  /**
    * Encoders to assist converting a csv records into Case Classes.
    */
  implicit val approvalEncoder: Encoder[Approval] = Encoders.product[Approval]
  implicit val usEncoder: Encoder[Us] = Encoders.product[Us]
  implicit val usStatesEncoder: Encoder[UsStates] = Encoders.product[UsStates]
  implicit val usCountiesEncoder: Encoder[UsCounties] = Encoders.product[UsCounties]

  /**
    * Create Spark collections.
    */
  def approvalRdd: RDD[Approval] = spark.read.options(csvReadOptions).csv("data/approval-topline.csv").as[Approval].rdd
  def usRdd: RDD[Us] = spark.read.options(csvReadOptions).csv("data/us.csv").as[Us].rdd
  def usStatesRdd: RDD[UsStates] = spark.read.options(csvReadOptions).csv("data/us-states.csv").as[UsStates].rdd
  def usCountiesRdd: RDD[UsCounties] = spark.read.options(csvReadOptions).csv("data/us-counties.csv").as[UsCounties].rdd

  test("Question 1") {
    val result = Deliverable.question1(usCountiesRdd)
    result.length must equal(1)
    result must contain("Hennepin")
  }

  test("Question 2") {
    val result = Deliverable.question2(usCountiesRdd)
    result.county must equal("Ramsey")
    result.date must equal("2020-03-21 00:00:00")
  }

  test("Question 3") {
    val result = Deliverable.question3(usCountiesRdd, usStatesRdd)
    result.length must equal(1)
    result must contain("New York City")
  }

  test("Question 4") {
    val result = Deliverable.question4(approvalRdd, usRdd)
    result.length must equal(1)
    result must contain(43.452523)

  }

  test("Question 5") {
    val result = Deliverable.question5(approvalRdd, usRdd)
    result.length must equal(1)
    result must contain(43.428729)
  }

}
