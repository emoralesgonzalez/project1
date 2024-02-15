package org.grupo1.ejercicios.chapters4and5

import org.apache.spark.sql.functions._
import org.grupo1.compartida.Session

object Ej2_Flights {
  def e {

    val spark = Session.s()

    val delaysPath = "src/main/resources/departuredelays.csv"
    val airportsPath = "src/main/resources/airport-codes-na.txt"

    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")

    val delays = spark.read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")
/*
    val foo= delays.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0"""))
    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo").show()

    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()

    spark.sql(
      """SELECT *
        FROM bar
        WHERE origin = 'SEA'
        AND destination = 'SFO'
        AND date LIKE '01010%'
        AND delay > 0""").show()

  foo.join(airports.as("air"), col("air.IATA") === col("origin"))
    .select("City", "State", "date", "distance", "destination")
    .show()

*/
    spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")
    spark.sql("""CREATE TABLE departureDelaysWindow AS
             SELECT origin, destination, SUM(delay) AS TotalDelays
             FROM departureDelays
             WHERE origin IN ('SEA', 'SFO', 'JFK')
             AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
             GROUP BY origin, destination;""")

    spark.sql(
      """SELECT origin, destination, TotalDelays, rank
         FROM (SELECT origin, destination, TotalDelays, dens_rank()
                 OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
                 FROM departureDelaysWindow) t
         WHERE rank <= 3""").show()

/*
    val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
    foo2.show()

    val foo3 = foo2.drop("delay")
    foo3.show()

    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    spark.sql(
      """SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
        FROM departureDelays
        WHERE origin = 'SEA'""").show()

    spark.sql(
      """SELECT * FROM (
         SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
         FROM departureDelays
         WHERE origin = 'SEA')
         PIVOT (
          CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
          FOR month in (1 JAN, 2 FEB)
         )
         ORDER BY destination
        """).show()*/
  }
}
