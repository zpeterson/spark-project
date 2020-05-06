package com.spark.project.dto

case class Approval(
    president: String,
    subgroup: String,
    model_date: String,
    approve_estimate: Double,
    approve_hi: Double,
    approve_loL: Double,
    disapprove_estimate: Double,
    disapprove_hi: Double,
    disapprove_lo: Double,
    timestamp: String
)
