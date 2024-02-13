package org.grupo1.ejercicios.chapter3

import org.compartida.Session
import org.apache.spark.sql.types._
import org.ejercicios.chapter3._

import scala.io.StdIn

object Ej3_Fire {
  def e {
    val spark = Session.s()

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
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    println("¿Quieres ejecutar Ej3_1('1') o Ej3_2('2')?")
    var ej = StdIn.readLine()
    while (ej != "1" && ej != "2"){
      println("Error, elija 1 o 2:")
      ej = StdIn.readLine()
    }
    if (ej == "1"){
      Ej3_Fire_1.e(spark, fireDF)
    }else if (ej == "2"){
      Ej3_Fire_2.e(spark, fireDF)
    }
  }
}
