package org.grupo1.ejercicios.chapters4and5

import org.grupo1.compartida.Session

object Ej1_Flights {
  def e {
    val spark = Session.s()

    val csvFile="src/main/resources/departuredelays.csv"
    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """SELECT distance, origin, destination
         FROM us_delay_flights_tbl
         WHERE distance > 1000 ORDER BY distance DESC""").show(10)

    spark.sql(
      """SELECT date, delay, origin, destination
        FROM us_delay_flights_tbl
        WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
        ORDER BY delay DESC""").show(10)

    spark.sql("""SELECT FLOOR(date/1000000) AS month, FLOOR(date/10000)-FLOOR(date/1000000)*100 AS day,
             CONCAT_WS(':', FLOOR(date/100)-FLOOR(date/10000)*100, date-FLOOR(date/100)*100) AS hour, delay, origin, destination
             FROM us_delay_flights_tbl
             WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
             ORDER BY delay DESC""").show(10)

    spark.sql("""SELECT delay, origin, destination,
                 CASE
                   WHEN delay > 360 THEN 'Very Long Delays'
                   WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                   WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                   WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                   WHEN delay = 0 THEN 'No Delays'
                   ELSE 'Early'
                 END AS Flight_Delays
                 FROM us_delay_flights_tbl
                 ORDER BY origin, delay DESC""").show(10)

    /*
    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

    spark.sql("SELECT * FROM managed_us_delay_flights_tbl").show()

    */


    /*
    spark.sql(s"""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)
             USING csv OPTIONS (PATH '$csvFile')""")

    spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
             SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin = 'SFO';""")

    spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
             SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin = 'JFK'""")

    spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view")

    spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show()

    spark.catalog.listDatabases()
    spark.catalog.listTables()
    spark.catalog.listColumns("us_delay_flights_tbl")
    */
  }
}
