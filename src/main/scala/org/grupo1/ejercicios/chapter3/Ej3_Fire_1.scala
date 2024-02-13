package org.ejercicios.chapter3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Ej3_Fire_1 {


    def e(spark: SparkSession, fireDF: DataFrame) {

      val fewFireDF = fireDF
        .select("IncidentNumber", "AvailableDtTm", "CallType")
        .where(col("CallType") =!= "Medical Incident")
      fewFireDF.show(5, false)

      fireDF
        .select("CallType")
        .where(col("CallType").isNotNull)
        .agg(countDistinct("CallType") as "DistinctCallTypes")
        .show()

      fireDF
        .select("CallType")
        .where(col("CallType").isNotNull)
        .distinct()
        .show(10, false)

      val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
      newFireDF
        .select("ResponseDelayedinMins")
        .where(col("ResponseDelayedinMins") > 5)
        .show(5, false)

      val fireTsDF = newFireDF
        .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
          "MM/dd/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm")

      fireTsDF
        .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
        .show(5, false)

      fireTsDF
        .select(year(col("IncidentDate")))
        .distinct()
        .orderBy(year(col("IncidentDate")))
        .show()

      fireTsDF
        .select("CallType")
        .where(col("CallType").isNotNull)
        .groupBy("CallType")
        .count()
        .orderBy(desc("count"))
        .show(10, false)

      fireTsDF
        .select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"), max("ResponseDelayedinMins"))
        .show()
    }
}
