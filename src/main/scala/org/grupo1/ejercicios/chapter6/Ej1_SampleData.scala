package org.grupo1.ejercicios.chapter6


import org.grupo1.compartida.Session
import org.grupo1.datasets._
import scala.util.Random
import org.apache.spark.sql.functions._


object Ej1_SampleData {
  def e {

    val spark = Session.s()

    import spark.implicits._

    val r = new Random(42)

    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    dsUsage.filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    dsUsage.map(u => {if (u.usage >750) u.usage * .15 else u.usage * .50})
      .show(5, false)

    dsUsage.map(u => {computeUserCostUsage(u)}).show(5)

  }
  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
      UsageCost(u.uid, u.uname, u.usage, v)
  }
}
