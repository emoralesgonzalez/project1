package org.ejercicios

import org.compartida.Session
import org.apache.spark.sql.functions._

object Mnm {
  def e(s: Session){
    val mnmFile = "src/main/resources/mnm_dataset.csv"

    val mnmDF = s.spark.read.format("csv")
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

    s.spark.stop()
  }
}
