package com.spark.scala.job

import com.spark.scala.shared.BaseUtils.{logger, readDataFrame, renameAndDelete, writeDataFrame}
import com.spark.scala.shared.SparkSessionObject.sparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{sum, trim}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object MainDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    // Creating spark Session
    //val sparkSession = SparkSessionObject.sparkSession

    val s1 = args(0) // path for file brazil_covid19_cities.csv including filename
    val s2 = args(1) // path for file brazil_covid19.csv including filename
    val target = args(2) // path to save new_brazil_covid19 file
    //val target2 = args(3) // path to save report-diff.json file

    // Reading brazil_covid19_cities.csv file
    val sourceOne = readDataFrame(s1,"csv")(sparkSession)

    //sourceOne.show(10,false)
    //sourceOne.printSchema()

    // Reading brazil_covid19.csv file
    val sourceTwo = readDataFrame(s2,"csv")(sparkSession)

    // Getting region state mapping
    val regionStateMapping = sourceTwo.select("region", "state").distinct().cache()

    //sourceTwo
    //  .show(10, false)
    //sourceTwo.printSchema()

    // grouping brazil_covid19_cities.csv dataset based on date and state to align it with brazil_covid19.csv dataset
    val firstDataset = sourceOne
      .select("date", "state", "cases", "deaths")
      .groupBy("date", "state")
      .agg(sum("cases").alias("cases"),
        sum("deaths").alias("deaths"))

    // Adding region column to brazil_covid19_cities.csv dataset
    val firstDataSetWithRegion = firstDataset.join(regionStateMapping, firstDataset("state") === regionStateMapping("state"), "inner")
      .select(firstDataset("date"), regionStateMapping("region"), firstDataset("state"), firstDataset("cases"), firstDataset("deaths"))

    //firstDataSetWithRegion.show(10,false)
    // Writing final dataset
    writeDataFrame(firstDataSetWithRegion,target,"csv")

    // Renaming file to correct name
    renameAndDelete("part-00000*.csv","new_brazil_covid19.csv",target)(sparkSession)
    println("Total Region state count: " + regionStateMapping.count)

    // Reading new_brazil_covid19.csv file
    val readOutputFile = readDataFrame(target + "/new_brazil_covid19.csv","csv")(sparkSession)

/*    logger.info("Finding diff:")
    //Finding difference in dataset new_brazil_covid19.csv and brazil_covid19.csv
    val findRecordDiff = readOutputFile
      .join(sourceTwo, trim(readOutputFile("date")) === trim(sourceTwo("date"))
        && trim(readOutputFile("region")) === trim(sourceTwo("region"))
        && trim(readOutputFile("state")) === trim(sourceTwo("state"))
      )
      .select(readOutputFile("date"), readOutputFile("region"), readOutputFile("state"), (readOutputFile("cases") - sourceTwo("cases")).alias("CasesDiff"), (readOutputFile("deaths") - sourceTwo("deaths")).alias("DeathsDiff"))

    // Writing difference to json file
    writeDataFrame(findRecordDiff,target2,"json")

    logger.info("Diff calculation completed:")

    // Renaming file to correct name
    renameAndDelete("part-00000*.json","report-diff.json",target2)(sparkSession)*/
  }

}
