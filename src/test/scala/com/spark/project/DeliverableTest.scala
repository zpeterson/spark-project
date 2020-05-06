package com.spark.project

import com.spark.project.dto.{Approval, Us, UsCounties, UsStates}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class DeliverableTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  /**
    * Question 1: Which county in Minnesota has the most reported
    * Covid-19 cases (as of 2020-04-25)?
    */
  test("Question 1") {
    Deliverable.question1(usCountiesRdd) must equal("Hennepin")
  }

  /**
    * Question 2: Which county in Minnesota was the first to report
    * a Covid-19 death?
    */
  test("Question 2") {
    Deliverable.question2(usCountiesRdd) must equal("Ramsey")
  }

  /**
    * Question 3: In the state with the most reported Covid-19 deaths
    * (as of 2020-04-25), which county has the most Covid-19 cases?
    */
  test("Question 3") {
    Deliverable.question3(usCountiesRdd, usStatesRdd) must equal("New York City")
  }

  /**
    * Question 4: What was Trump’s approval rating in the round of polls
    * directly before the US reported its first Covid-19 death (on 2020-02-29)?
    */
  test("Question 4") {
    Deliverable.question4(approvalRdd, usRdd) must equal(43.452523)
  }

  /**
    * Question 5: What was Trump’s approval rating in the round of polls
    * directly following the US reporting its highest number of Covid-19
    * deaths (as of 2020-04-25)?
    */
  test("Question 5") {
    Deliverable.question5(approvalRdd, usRdd) must equal(43.428729)
  }

  /**
    * Create a SparkSession that runs locally.
    */
  val spark =
    SparkSession.builder().appName("Project App").master("local[*]").getOrCreate()

  /**
    * Encoders to assist converting a csv records into Case Classes.
    */
  implicit val approvalEncoder: Encoder[Approval] = Encoders.product[Approval]
  implicit val usEncoder: Encoder[Us] = Encoders.product[Us]
  implicit val usStatesEncoder: Encoder[UsStates] = Encoders.product[UsStates]
  implicit val usCountiesEncoder: Encoder[UsCounties] = Encoders.product[UsCounties]

  /**
    * Let Spark infer the data types. Tell Spark this CSV has a header line.
    */
  val csvReadOptions = Map("inferSchema" -> true.toString, "header" -> true.toString)

  /**
    * Create Spark collections
    */
  def approvalRdd = spark.read.options(csvReadOptions).csv("data/approval-topline.csv").as[Approval].rdd
  def usRdd = spark.read.options(csvReadOptions).csv("data/us.csv").as[Us].rdd
  def usStatesRdd = spark.read.options(csvReadOptions).csv("data/us-states.csv").as[UsStates].rdd
  def usCountiesRdd = spark.read.options(csvReadOptions).csv("data/us-counties.csv").as[UsCounties].rdd

}
