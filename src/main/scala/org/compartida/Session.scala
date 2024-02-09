package org.compartida

import org.apache.spark.sql.SparkSession

class Session {
  val spark = SparkSession.builder
    .master("local")
    .appName("project1")
    .config("spark.some.config.option","some-value")
    .getOrCreate()

  spark.sparkContext
    .setLogLevel("ERROR")
}
