package com.spark.scala.shared

import org.apache.spark.sql.SparkSession

object SparkSessionObject {

  val sparkSession = SparkSession
    .builder()
    .appName("Spark Demo")
    .master("local[4]")
    .enableHiveSupport()
    .getOrCreate()
}
