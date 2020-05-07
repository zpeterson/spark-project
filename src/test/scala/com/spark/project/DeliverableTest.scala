package com.spark.project

import com.spark.project.dto.{Approval, Us, UsCounties, UsStates}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration._

class DeliverableTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val spark = SparkSession.builder().appName("Project").master("local[*]").getOrCreate()

  /**
    * Question 1: Which county in Minnesota has the most reported
    * Covid-19 cases (as of 2020-04-25)?
    */
  test("Question 1") {
    val result = Deliverable.question1(usCountiesRdd)
    result.length must equal(1)
    result must contain("Hennepin")
  }

  /**
    * Question 2: Which county in Minnesota was the first to report
    * a Covid-19 death?
    */
  test("Question 2") {
    val result = Deliverable.question2(usCountiesRdd)
    result.county must equal("Ramsey")
    result.date must equal("2020-03-21 00:00:00")
  }

  /**
    * Question 3: In the state with the most reported Covid-19 deaths
    * (as of 2020-04-25), which county has the most Covid-19 cases?
    */
  test("Question 3") {
    val result = Deliverable.question3(usCountiesRdd, usStatesRdd)
    result.length must equal(1)
    result must contain("New York City")
  }

  /**
    * Question 4: What was Trump’s approval rating in the round of polls
    * directly before the US reported its first Covid-19 death (on 2020-02-29)?
    */
  test("Question 4") {
    val result = Deliverable.question4(approvalRdd, usRdd)
    result.length must equal(1)
    result must contain(43.452523)

  }

  /**
    * Question 5: What was Trump’s approval rating in the round of polls
    * directly following the US reporting its highest number of Covid-19
    * deaths (as of 2020-04-25)?
    */
  test("Question 5") {
    val result = Deliverable.question5(approvalRdd, usRdd)
    result.length must equal(1)
    result must contain(43.428729)
  }

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
    * Create Spark collections.
    */
  def approvalRdd: RDD[Approval] = spark.read.options(csvReadOptions).csv("data/approval-topline.csv").as[Approval].rdd
  def usRdd: RDD[Us] = spark.read.options(csvReadOptions).csv("data/us.csv").as[Us].rdd
  def usStatesRdd: RDD[UsStates] = spark.read.options(csvReadOptions).csv("data/us-states.csv").as[UsStates].rdd
  def usCountiesRdd: RDD[UsCounties] = spark.read.options(csvReadOptions).csv("data/us-counties.csv").as[UsCounties].rdd

  /**
    * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
    * NOTE: If you use this, you must terminate your test manually.
    * OTHER NOTE: You should only use this if you run a test individually.
    */
  val BLOCK_ON_COMPLETION = false;

  /**
    * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
    * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
    */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

}
