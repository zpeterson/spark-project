package com.spark.project

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.spark.project.dto.{Approval, Us, UsCounties, UsStates}
import org.apache.spark.rdd.RDD

object Deliverable {

  private val ALL_POLLS = "All polls"
  private val MINNESOTA = "Minnesota"
  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
    * Question 1: Which county in Minnesota has the most reported
    * Covid-19 cases (as of 2020-04-25)?
    *
    * @param countyData
    * @return the county with the most Covid-19 cases in Minnesota
    */
  def question1(countyData: RDD[UsCounties]): Array[String] = {
    val mnCounties = countyData.filter(x => x.state.equals(MINNESOTA))
    val mnMaxCases = maxCasesByState(mnCounties)
    countyWithTheMostCases(mnCounties, mnMaxCases)
  }

  /**
    * Question 2: Which county in Minnesota was the first to report
    * a Covid-19 death?
    *
    * @param countyData
    * @return the county that reported the first Covid-19 death
    *         in Minnesota
    */
  def question2(countyData: RDD[UsCounties]): UsCounties = {
    countyData.filter(x => x.state.equals(MINNESOTA) && x.deaths != 0).sortBy(y => y.date).first()
  }

  /**
    * Question 3: In the state with the most reported Covid-19 deaths
    * (as of 2020-04-25), which county has the most Covid-19 cases?
    *
    * @param countyData
    * @param stateData
    * @return the county with the most Covid-19 cases from the state
    *         with the most reported Covid-19 deaths
    */
  def question3(countyData: RDD[UsCounties], stateData: RDD[UsStates]): Array[String] = {
    //death data
    val maxDeath = stateData.map(x => x.deaths).max()
    val maxDeathState = stateData.filter(y => y.deaths.equals(maxDeath)).first().state

    // county data
    val counties = countyData.filter(x => x.state.equals(maxDeathState))
    val maxCases = maxCasesByState(counties)

    countyWithTheMostCases(counties, maxCases);
  }

  /**
    * Question 4: What was Trump’s approval rating in the round of polls
    * directly before the US reported its first Covid-19 death?
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
          && y.subgroup.equals(ALL_POLLS)
      )
      .map(y => y.approve_estimate)
      .collect()
  }

  /**
    * Question 5: What was Trump’s approval rating in the round of polls
    * directly following the US reporting its highest number of Covid-19
    * deaths (as of 2020-04-25)?
    *
    * @param approvalData
    * @param usData
    * @return the president's approval rating from the day after the
    *         US reported its highest number of Covid-19 deaths
    */
  def question5(approvalData: RDD[Approval], usData: RDD[Us]): Array[Double] = {
    val maxDeath = usData.map(x => x.deaths).max()
    val maxDeathDay = usData.filter(y => y.deaths.equals(maxDeath)).first().date
    approvalData
      .filter(z =>
        parseTimestamp(z.model_date).equals(parseTimestamp(maxDeathDay).plusDays(1))
          && z.subgroup.equals(ALL_POLLS)
      )
      .map(a => a.approve_estimate)
      .collect()
  }

  /**
    * Helper function to get the most reported cases from a given
    * state's counties.
    *
    * @param counties
    * @return
    */
  private def maxCasesByState(counties: RDD[UsCounties]): Int = {
    counties.map(y => y.cases).max()
  }

  /**
    * Helper function to get the county with the most reported cases
    * in a given state.
    *
    * @param counties
    * @param maxCases
    * @return
    */
  private def countyWithTheMostCases(counties: RDD[UsCounties], maxCases: Int): Array[String] = {
    counties.filter(x => x.cases.equals(maxCases)).map(y => y.county).collect()
  }

  /**
    * Helper function to parse date strings.
    *
    * @param timestamp
    * @return
    */
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))

}
