package org.ejercicios.chapter3

import org.apache.spark.sql.functions._
import org.compartida.Session

object Ej1_1_dfBasico {
 def e: Unit = {
   val spark = Session.s()
   val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
     ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

   val avgDF = dataDF.groupBy("name").agg(avg("age"))

   avgDF.show()
 }
}
