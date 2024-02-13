package org.ejercicios.chapter3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Ej3_Fire_2 {

  def e(spark: SparkSession, fireDF: DataFrame)   {



  val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")

  val fireTsDF = newFireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
      "MM/dd/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm")

  fireTsDF
    .select("CallType")
    .where(year(col("IncidentDate")) === 2018)
    .distinct()
    .show()

  fireTsDF
    .where(year(col("IncidentDate")) === 2018)
    .where(col("CallFinalDisposition") like "Fire")
    .groupBy(month(col("IncidentDate")))
    .count()
    .orderBy(month(col("IncidentDate")))
    .show()


  fireTsDF
    .where(year(col("IncidentDate")) === 2018)
    .where(col("CallFinalDisposition") like "Fire")
    .groupBy(col("Neighborhood"))
    .count()
    .show()

    fireTsDF
      .where(year(col("IncidentDate")) === 2018)
      .where(col("CallFinalDisposition") like "Fire")
      .groupBy(col("Neighborhood"))
      .avg("ResponseDelayedinMins")
      .orderBy(desc("avg(ResponseDelayedinMins)"))
      .show()

    fireTsDF
      .where(year(col("IncidentDate")) === 2018)
      .where(col("CallFinalDisposition") like "Fire")
      .groupBy(weekofyear(col("IncidentDate")))
      .count()
      .orderBy(desc("count"))
      .show(1)

    fireTsDF
      .where(col("CallFinalDisposition") === "Fire")
      .groupBy("Neighborhood", "Zipcode")
      .count()
      .orderBy("Zipcode", "Neighborhood")
      .show(100)

    val parquetPath = "src/main/resources/parquet"
    fireDF.write.mode("Overwrite").parquet(parquetPath)

    val parquetRead = spark.read.parquet(parquetPath)

    parquetRead.show()

  }

}
