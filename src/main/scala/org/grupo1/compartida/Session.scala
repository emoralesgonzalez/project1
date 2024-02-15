package org.grupo1.compartida

import org.apache.spark.sql.SparkSession

object Session {
  def s(m: String, a: String, c1: String, c2: String): SparkSession = {
    val spark = SparkSession.builder()
      .master(m)
      .appName(a)
      .config(c1,c2)
      /*.config("spark.sql.catalogImplementation","hive")*/
      .enableHiveSupport()
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
      /*.config("spark.sql.catalogImplementation","hive")*/
      .enableHiveSupport()
      /*https://stackoverflow.com/questions/54967186/cannot-create-table-with-spark-sql-hive-support-is-required-to-create-hive-tab
      */
      .getOrCreate()

    spark.sparkContext
      .setLogLevel("ERROR")

    return spark

  }
}
