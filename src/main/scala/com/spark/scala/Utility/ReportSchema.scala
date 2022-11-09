package com.spark.scala.Utility

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

trait ReportSchema {

  val reportSchema = StructType(
    Array(
      StructField("source_count", StringType,true),
      StructField("destination_count", StringType,true),
      StructField("data_in_source_but_not_in_destination", StringType,true),
      StructField("data_in_destination_but_not_in_source", StringType,true),
      StructField("data_same_in_source_destination", StringType,true),
      StructField("data_different_in_source_destination", StringType,true)
    ))

}
