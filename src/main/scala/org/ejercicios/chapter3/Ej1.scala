package org.ejercicios.chapter3

import org.compartida.Session

object Ej1 {
  def e {

    val spark = Session.s("local", "project1", "spark.some.config.option","some-value")

    val data = Seq(
      (1, "Hola"),
      (2, "Hola"),
      (3, "Hola")
    )


    val df = spark.createDataFrame(data).toDF("ID", "info")

    df.show()
  }
}
