package com.spark.scala.job

import com.spark.scala.shared.BaseUtils
import com.spark.scala.Utility._
import com.spark.scala.shared.BaseUtils.{renameAndDelete, writeDataFrame}
import org.slf4j.LoggerFactory
import com.spark.scala.shared.SparkSessionObject.sparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, concat, lit, sum}

import scala.collection.JavaConversions._

object Report extends ReportSchema{
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val source = args(0)
    val destination = args(1)
    val jsonFileLocToWrite = args(2)

    val readSource = BaseUtils.readDataFrame(source,"csv")(sparkSession)
      .withColumn("date_state",concat(col("date"),col("state")))
    val readDestination = BaseUtils.readDataFrame(destination,"csv")(sparkSession)
      .withColumn("date_state",concat(col("date"),col("state")))


    println("Source Count: " + readSource.count)
    println("Destination Count: " + readDestination.count)

    val recordInSourceNotInDestination = readSource.alias("rs")
      .join(readDestination,readSource("date_state") === readDestination("date_state"),"left")
      .filter(readDestination("date_state").isNull)
      .selectExpr("rs.*")
      .drop(col("date_state"))

    recordInSourceNotInDestination.show(10,false)

    println("Source Count: " + recordInSourceNotInDestination.count)

    val recordInDestinationNotInSource = readDestination.alias("rd")
      .join(readSource,readSource("date_state") === readDestination("date_state"),"left")
      .filter(readSource("date_state").isNull)
      .selectExpr("rd.*")
      .drop(col("date_state"))

    recordInDestinationNotInSource.show(10,false)

    println("Source Count: " + recordInDestinationNotInSource.count)

    val sameCasesAndDeaths = readSource
      .join(readDestination,readSource("date_state") === readDestination("date_state")
        && readSource("cases") === readDestination("cases")
        && readSource("deaths") === readDestination("deaths")
        ,"inner")

    println("same Cases And Deaths: " + sameCasesAndDeaths.count)

    val sameKeyRecords = readSource
      .join(readDestination,readSource("date_state") === readDestination("date_state"),"inner")

    println("same key records: " + sameKeyRecords.count)

    val differentCasesAndDeaths = sameKeyRecords.count() - sameCasesAndDeaths.count()

    println("Different Cases And Deaths: " + differentCasesAndDeaths)

    val reportData = Seq(Row(readSource.count.toString
      ,readDestination.count.toString
      ,recordInSourceNotInDestination.count.toString
      ,recordInDestinationNotInSource.count.toString
      ,sameCasesAndDeaths.count.toString
      ,differentCasesAndDeaths.toString
    ))

    val  reportDF = sparkSession
      .createDataFrame(reportData,reportSchema)

    reportDF.show(10,false)

    writeDataFrame(reportDF,jsonFileLocToWrite,"json")

    renameAndDelete("part-00000*.json","report-diff.json",jsonFileLocToWrite)(sparkSession)

  }


}
