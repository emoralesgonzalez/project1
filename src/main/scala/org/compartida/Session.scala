package org.compartida

import org.apache.spark.sql.SparkSession

object Session {
  def s(m: String, a: String, c1: String, c2: String): SparkSession = {
    val spark = SparkSession.builder()
      .master(m)
      .appName(a)
      .config(c1,c2)
      .getOrCreate()

    spark.sparkContext
      .setLogLevel("ERROR")

    return spark
  }

  def s(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("project1")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    spark.sparkContext
      .setLogLevel("ERROR")

    return spark

  }
}
