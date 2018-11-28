package com.hashmap

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object CSVReader {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Full Employment Test UI").setMaster("local[*]")
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    val districtCode = sparkSession.sqlContext.read
      .format("csv")
      .option("treatEmptyValuesAsNulls", "true")
      .option("header", true)
      .option("inferSchema", "true")
      .load("E:\\SparkStreaming\\Districts Codes 2001.csv")


    val affectedAreas = sparkSession.sqlContext.read
      .format("csv")
      .option("treatEmptyValuesAsNulls", "false")
      .option("header", true)
      .option("inferSchema", "true")
      .load("E:\\SparkStreaming\\IndiaAffectedWaterQualityAreas.csv")

    import sparkSession.implicits._
    //First Problem
    val areasAffected = affectedAreas.withColumnRenamed("State Name", "State_Name")
      .withColumnRenamed("District Name", "District_Name")
      .withColumnRenamed("Block Name", "Block_Name")
      .withColumnRenamed("Panchayat Name", "Panchayat_Name")
      .withColumnRenamed("Village Name", "Village_Name")
      .withColumnRenamed("Habitation Name", "Habitation_Name")
      .withColumnRenamed("Quality Parameter", "Quality_Parameter")
      .withColumn("District_Name", Utils.removeCode($"District_Name"))
      .withColumn("Block_Name", Utils.removeCode($"Block_Name"))
      .withColumn("Panchayat_Name", Utils.removeCode($"Panchayat_Name"))
      .withColumn("Village_Name", Utils.removeCode($"Village_Name"))
      .withColumn("Habitation_Name", Utils.removeCode($"Habitation_Name"))
      .withColumn("Joining_Column" , lit("1"))

    val df1 = districtCode
      .withColumnRenamed("District Code", "District_Code")
      .withColumnRenamed("State Code", "State_Code")
      .withColumn("District_Code", when(col("District_Code") === "0", "-999").otherwise($"District_Code"))
      .withColumn("Joining_Column" , lit("1"))


    val indiaAffectedWaterArea = areasAffected.crossJoin(df1).drop("Joining_column")
      .withColumn("Year", to_date($"Year", "dd/MM/yy").cast(DataTypes.DateType))
      .filter($"Name of the State/Union territory and Districts".contains("*") === false)
      .drop("Name of the State/Union territory and Districts")

    indiaAffectedWaterArea.show()

    //addTableToHive(indiaAffectedWaterArea)
    println("------------------Table added-----------------")


    //Second Problem
    val totalCount = indiaAffectedWaterArea.withColumn("Quality_Parameter" , count($"Quality_Parameter"))
      .withColumn("Frequency" , count($"Quality_Parameter")/12 )
      .select(col("Village_Name") , col("Quality_Parameter") , col("Year") , col("Frequency"))

    totalCount.show
   // addTableToHiveTable(totalCount)

  }
  def addTableToHive(dataframe: DataFrame): Unit = {
    dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("demo.india_affected_water_area")
  }
  def addTableToHiveTable(dataframe: DataFrame): Unit = {
    dataframe.write.format("orc").mode(SaveMode.Append).saveAsTable("demo.total_Count_QParameter")
  }
}

