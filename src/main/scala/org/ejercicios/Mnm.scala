package org.ejercicios

import org.apache.spark.sql.functions._
import org.compartida.Session

object Mnm {
  def e{

    val spark = Session.s()

    val mnmFile = "src/main/resources/mnm_dataset.csv"

    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(10)

    spark.stop()
  }
}
