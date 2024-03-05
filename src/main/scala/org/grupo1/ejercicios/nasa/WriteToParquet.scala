package org.grupo1.ejercicios.nasa

import org.apache.spark.sql.functions._
import org.grupo1.compartida.Session

object WriteToParquet {
  def e(archivo: String, path_to_parquet: String) {
    val spark = Session.s()

    val df_base = spark.read.text(archivo)

    val df = df_base.select(regexp_extract(col("value"), """^([^(\s|,)]+)""", 1).alias("host"),
      regexp_extract(col("value"), """^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1).as("date"),
      regexp_extract(col("value"), """"([^\s]+)""", 1).as("request_method"),
      regexp_extract(col("value"), """^.*"\w+\s([^\s]+)""", 1).as("resource"),
      regexp_extract(col("value"), """^.*\s(\w+\/\d+\.\d+)""", 1).as("protocol"),
      regexp_extract(col("value"), """^.*"\s([\d]+)""", 1).cast("int").alias("http_status_code"),
      regexp_extract(col("value"), """\s([\d]+|-)$""",1).as("size"))
      .withColumn("size", regexp_replace(col("size"), "-", "0"))
      .withColumn("size", col("size").cast("int"))
      .withColumn("date", regexp_replace(col("date"), "Jan", "01"))
      .withColumn("date", regexp_replace(col("date"), "Feb", "02"))
      .withColumn("date", regexp_replace(col("date"), "Mar", "03"))
      .withColumn("date", regexp_replace(col("date"), "Apr", "04"))
      .withColumn("date", regexp_replace(col("date"), "May", "05"))
      .withColumn("date", regexp_replace(col("date"), "Jun", "06"))
      .withColumn("date", regexp_replace(col("date"), "Jul", "07"))
      .withColumn("date", regexp_replace(col("date"), "Aug", "08"))
      .withColumn("date", regexp_replace(col("date"), "Sep", "09"))
      .withColumn("date", regexp_replace(col("date"), "Oct", "10"))
      .withColumn("date", regexp_replace(col("date"), "Nov", "11"))
      .withColumn("date", regexp_replace(col("date"), "Dec", "12"))
      .withColumn("date", to_timestamp(col("date"), "dd/MM/yyyy:HH:mm:ss"))
    df.show()
    df.printSchema()

    val bad_rows = df.where(col("host").isNull or col("date").isNull or col("request_method").isNull or col("resource").isNull or col("protocol").isNull or col("http_status_code").isNull or col("size").isNull)
      .count()
    println("Bad rows: " + bad_rows)

    val df_clean = df.na.drop()
    df_clean.show()

    df_clean.write.mode("overwrite").parquet(path_to_parquet)


  }
}
