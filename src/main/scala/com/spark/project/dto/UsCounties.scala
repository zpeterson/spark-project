package com.spark.project.dto

case class UsCounties(
    date: String,
    county: String,
    state: String,
    fips: String,
    cases: Int,
    deaths: Int
)
