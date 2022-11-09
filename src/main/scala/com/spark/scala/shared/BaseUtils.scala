package com.spark.scala.shared
import com.spark.scala.shared.SparkSessionObject.sparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object BaseUtils {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def renameAndDelete(patternString: String,newFileName: String,target: String)(spark: SparkSession): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(target + "/" + patternString))(0).getPath()
    fs.rename(file, new Path(target + "/" + newFileName))
    logger.info(s"${patternString} pattern has been successfully renamed with ${newFileName}")
    fs.delete(new Path(target + "/_SUCCESS"), true)
    logger.info(s"_SUCCESS file successfully deleted")

  }

  def readDataFrame(path: String,fileType: String)(spark: SparkSession): DataFrame = {

    if(fileType == "csv") {
      val dataframe = spark
        .read
        .format("csv")
        .option("inferSchema", true)
        .option("sep", ",")
        .option("header", true)
        .load(path)

      logger.info(s"Dataframe created successfully")

      dataframe
    }
    else if(fileType == "json") {
      val dataframe = spark
        .read
        .format("json")
        .option("inferSchema", true)
        .load(path)

      logger.info(s"Dataframe created successfully")

      dataframe
    }
    else{
      logger.info("Unsupported file type")
      val dataframe = sparkSession.emptyDataFrame

      dataframe
    }

  }

  def writeDataFrame(dataframe: DataFrame,path: String,fileType: String, partitionValue: Int=1)={

    if(fileType == "json") {
      dataframe
        .repartition(partitionValue)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .json(path)

      logger.info("Dataframe written successfully to JSON!!")
    }
    if(fileType == "csv")  {
      dataframe
        .repartition(partitionValue)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv(path)

      logger.info("Dataframe written successfully to CSV!!")
    }
    else {
      logger.info("Invalid file type to write!")

    }
  }
}
