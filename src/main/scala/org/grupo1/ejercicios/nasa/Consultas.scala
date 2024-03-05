package org.grupo1.ejercicios.nasa

import org.apache.spark.sql.functions._
import org.grupo1.compartida.Session

object Consultas {
  def e(parquet_path: String) {
    val spark = Session.s()

    val df = spark.read.parquet(parquet_path)

    println("¿Cuáles son los distintos protocolos web utilizados? Agrúpalos")
    df.groupBy("protocol")
      .count()
      .show()

    println("¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.")
    df.groupBy("http_status_code")
      .count()
      .orderBy(desc("count"))
      .show()

    println("¿Y los métodos de petición (verbos) más utilizados?")
    df.groupBy("request_method")
      .count()
      .orderBy(desc("count"))
      .show()

    println("¿Qué recurso tuvo la mayor transferencia de bytes de la página web?")
    df.select(col("resource"), col("size"))
      .orderBy(desc("size"))
      .show(1)

    println("Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.")
    df.groupBy("resource")
      .count()
      .orderBy(desc("count"))
      .show(1)

    println("¿Qué días la web recibió más tráfico?")
    df.groupBy(dayofmonth(col("date")))
      .count()
      .orderBy(desc("count"))
      .show(31)

    println("¿Cuáles son los hosts más frecuentes?")
    df.groupBy("host")
      .count()
      .orderBy(desc("count"))
      .show()

    println("¿A qué horas se produce el mayor número de tráfico en la web?")
    df.groupBy(hour(col("date")))
      .count()
      .orderBy(desc("count"))
      .show(24)

    println("¿Cuál es el número de errores 404 que ha habido cada día?")
    df.where(col("http_status_code") === 404)
      .groupBy(dayofmonth(col("date")))
      .sum("http_status_code")
      .show()
  }
}
