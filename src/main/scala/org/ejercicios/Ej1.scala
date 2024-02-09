package org.ejercicios

import org.compartida.Session

object Ej1 {
  def e(s: Session) {
    val data = Seq(
      (1, "Hola"),
      (2, "Hola"),
      (3, "Hola")
    )


    val df = s.spark.createDataFrame(data).toDF("ID", "info")

    df.show()
  }
}
