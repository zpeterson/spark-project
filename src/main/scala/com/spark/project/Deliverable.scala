package com.spark.project

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.spark.project.dto.{Approval, Us, UsCounties, UsStates}
import org.apache.spark.rdd.RDD

object Deliverable {

  /**
    * To find the county in Minnesota with the most reported cases of
    * Covid-19 (as of 2020-04-25), filter the dataset to records where
    * the UsCounties#state() is equal to “Minnesota,” then find the
    * max from UsCounties#cases().
    *
    * @param countyData
    * @return the county with the most Covid-19 cases in Minnesota
    */
  def question1(countyData: RDD[UsCounties]): Array[String] = {
    val maxCases = maxCasesByState(countyData, MINNESOTA)
    countyWithTheMostCases(countyData, MINNESOTA, maxCases)
  }

  /**
    * To find the first county in Minnesota to report a Covid-19 death,
    * filter the dataset to records where the UsCounties#state() is
    * equal to “Minnesota” and order the records by UsCounties#date().
    * Then, find the first record where UsCounties#deaths() is not
    * equal to 0.
    *
    * @param countyData
    * @return the county that reported the first Covid-19 death
    *         in Minnesota
    */
  def question2(countyData: RDD[UsCounties]): UsCounties = {
    countyData.filter(x => x.state.equals(MINNESOTA) && x.deaths != 0).sortBy(y => y.date).first()
  }

  /**
    * To find the county with the most Covid-19 cases in the state
    * with the most Covid-19 deaths (as of 2020-04-25), first take the
    * max from UsStates#deaths() and find the corresponding
    * UsStates#state(). Then, filter the countyData by that state
    * to find the max from UsCounties#cases().
    *
    * @param countyData
    * @param stateData
    * @return the county with the most Covid-19 cases from the state
    *         with the most reported Covid-19 deaths
    */
  def question3(countyData: RDD[UsCounties], stateData: RDD[UsStates]): Array[String] = {
    val maxDeath = stateData.map(x => x.deaths).max()
    val maxDeathState = stateData.filter(y => y.deaths == maxDeath).first().state
    val maxCases = maxCasesByState(countyData, maxDeathState)
    countyWithTheMostCases(countyData, maxDeathState, maxCases)
  }

  /**
    * To get Trump's approval rating in the round of polls directly
    * before the US reported its first Covid-19 death, first
    * order usData by Us#date() and find the first record where
    * Us#deaths() is not equal to 0. Then, find the Approval record
    * where Approval#model_date() is one day before the date of the
    * first reported US Covid-19 death, and the Approval#subgroup()
    * is equal to “All polls."
    *
    * @param approvalData
    * @param usData
    * @return the president's approval rating from the day
    *         before the US reported its first Covid-19 death
    */
  def question4(approvalData: RDD[Approval], usData: RDD[Us]): Array[Double] = {
    val firstDeathDate = usData.filter(x => x.deaths != 0).sortBy(y => y.date).first().date
    approvalData
      .filter(y =>
        parseTimestamp(y.model_date).equals(parseTimestamp(firstDeathDate).minusDays(1))
          && y.subgroup.equals("All polls")
      )
      .map(y => y.approve_estimate)
      .collect()
  }

  /**
    * To get Trump's approval rating in the round of polls directly
    * following the US reporting its highest number of Covid-19 deaths
    * (as of 2020-04-25), first find the max Us#deaths() and the
    * corresponding Us#date(). Then, find the record where the
    * Approval#model_date() is one day later and the
    * Approval#subgroup() is equal to “All polls.”
    *
    * @param approvalData
    * @param usData
    * @return the president's approval rating from the day after the
    *         US reported its highest number of Covid-19 deaths
    *         as of 2020-04-25
    */
  def question5(approvalData: RDD[Approval], usData: RDD[Us]): Array[Double] = {
    val maxDeath = usData.map(x => x.deaths).max()
    val maxDeathDay = usData.filter(y => y.deaths.equals(maxDeath)).first().date
    approvalData
      .filter(z =>
        parseTimestamp(z.model_date).equals(parseTimestamp(maxDeathDay).plusDays(1))
          && z.subgroup.equals("All polls")
      )
      .map(a => a.approve_estimate)
      .collect()
  }

  private val MINNESOTA = "Minnesota"
  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
    * Helper function to get the most reported cases in a given state.
    * @param state
    * @param countyData
    * @return
    */
  private def maxCasesByState(countyData: RDD[UsCounties], state: String): Int = {
    countyData.filter(x => x.state.equalsIgnoreCase(state)).map(y => y.cases).max()
  }

  /**
    * Helper function to get the county with the most reported cases in a given state.
    * @param countyData
    * @param maxCases
    * @param state
    * @return
    */
  private def countyWithTheMostCases(countyData: RDD[UsCounties], state: String, maxCases: Int): Array[String] = {
    countyData
      .filter(x =>
        x.cases.equals(maxCases)
          && x.state.equalsIgnoreCase(state)
      )
      .map(y => y.county)
      .collect()
  }

  /**
    * Helper function to parse date strings.
    * @param timestamp
    * @return
    */
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

}
