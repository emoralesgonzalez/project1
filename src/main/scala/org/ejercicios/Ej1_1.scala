package org.ejercicios

import org.apache.spark.sql.functions._
import org.compartida.Session

object Ej1_1 {
 def e(s: Session): Unit = {
   val dataDF = s.spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
     ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

   val avgDF = dataDF.groupBy("name").agg(avg("age"))

   avgDF.show()
 }
}
