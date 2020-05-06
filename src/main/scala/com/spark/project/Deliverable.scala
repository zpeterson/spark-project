package com.spark.project

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.spark.project.dto.{Approval, Us, UsCounties, UsStates}
import org.apache.spark.rdd.RDD

object Deliverable {

  /**
    * To find the county in Minnesota with the most reported cases of
    * Covid-19 (as of April 25), filter the dataset to records where
    * the UsCounties#state() is equal to “Minnesota,” then find the
    * max from UsCounties#cases(). Return the UsCounties#county().
    */
  def question1(countyData: RDD[UsCounties]): String = {
    ???
  }

  /**
    * To find the first county in Minnesota to report a Covid-19 death,
    * filter the dataset to records where the UsCounties#state() is
    * equal to “Minnesota” and order the records UsCounties#date().
    * Then, find the first record where UsCounties#deaths() is not
    * equal to 0. Return the UsCounties#county().
    */
  def question2(countyData: RDD[UsCounties]): String = {
    ???
  }

  /**
    * To find the county with the most Covid-19 cases from the state
    * with the most Covid-19 deaths (as of April 25), first join on
    * UsCounties#state() and UsStates#state(). Then, take the max from
    * UsStates#deaths(). Finally, filter the countyData by that state
    * to find the max from UsCounties#deaths(). Return the UsCounties#county().
    */
  def question3(countyData: RDD[UsCounties], stateData: RDD[UsStates]): String = {
    ???
  }

  /**
    * To get Trump's approval rating in the round of polls directly
    * before the US reported its first Covid-19 death, join on
    * Us#date() and Approval#model_date(). Order usData by Us#date()
    * and find the first record where Us#deaths() is not equal to 0.
    * Then, find the record where Approval#model_date() is one day
    * before the date of the first reported US Covid-19 death, and the
    * Approval#subgroup() is equal to “All polls." Return Approval#approve_estimate().
    */
  def question4(approvalData: RDD[Approval], usData: RDD[Us]): Double = {
    ???
  }

  /**
    * To get Trump's approval rating in the round of polls directly
    * following the US reporting its highest number of Covid-19 deaths
    * (as of April 25), join on Us#date() and Approval#model_date().
    * Find the record with the max Us#deaths(). Then, find the record
    * where the Approval#model_date() one day after the Us#date() from
    * that record and the Approval#subgroup() is equal to “All polls.”
    * Return Approval#approve_estimate().
    */
  def question5(approvalData: RDD[Approval], usData: RDD[Us]): Double = {
    ???
  }

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  /**
    * Helper function to parse the timestamp format used in the trip dataset.
    * @param timestamp
    * @return
    */
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
}
