package org.ejercicios

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.compartida.Session

object Ej3_2 {

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
      .select("Zipcode", "Neighborhood", "CallFinalDisposition")
      .where(col("CallFinalDisposition") === "Fire")
      .groupBy("Neighborhood", "Zipcode")
      .count()
      .orderBy("Zipcode", "Neighborhood")
      .show(100)


  }

}
