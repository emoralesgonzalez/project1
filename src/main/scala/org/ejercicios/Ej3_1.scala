package org.ejercicios

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.compartida.Session

object Ej3_1 {


    def e(s: Session) {

      val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
        StructField("UnitID", StringType, true),
        StructField("IncidentNumber", IntegerType, true),
        StructField("CallType", StringType, true),
        StructField("CallDate", StringType, true),
        StructField("WatchDate", StringType, true),
        StructField("CallFinalDisposition", StringType, true),
        StructField("AvailableDtTm", StringType, true),
        StructField("Address", StringType, true),
        StructField("City", StringType, true),
        StructField("Zipcode", IntegerType, true),
        StructField("Battalion", StringType, true),
        StructField("StationArea", StringType, true),
        StructField("Box", StringType, true),
        StructField("OriginalPriority", StringType, true),
        StructField("Priority", StringType, true),
        StructField("FinalPriority", IntegerType, true),
        StructField("ALSUnit", BooleanType, true),
        StructField("CallTypeGroup", StringType, true),
        StructField("NumAlarms", IntegerType, true),
        StructField("UnitType", StringType, true),
        StructField("UnitSequenceInCallDispatch", IntegerType, true),
        StructField("FirePreventionDistrict", StringType, true),
        StructField("SupervisorDistrict", StringType, true),
        StructField("Neighborhood", StringType, true),
        StructField("Location", StringType, true),
        StructField("RowID", StringType, true),
        StructField("Delay", FloatType, true)))

      val sfFireFile = "src/main/resources/sf-fire-calls.csv"
      val fireDF = s.spark.read.schema(fireSchema)
        .option("header", "true")
        .csv(sfFireFile)

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