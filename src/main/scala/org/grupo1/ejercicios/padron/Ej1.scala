package org.grupo1.ejercicios.padron

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.grupo1.compartida.Session

object Ej1 {
  def e {

    val spark = Session.s()

    val csvFile = "src/main/resources/estadisticas202402.csv"

    val df = spark.read
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(csvFile)
      .withColumn("DESC_DISTRITO", trim(col("DESC_DISTRITO")))
      .withColumn("DESC_BARRIO", trim(col("DESC_BARRIO")))
      .withColumn("COD_EDAD_INT", when(trim(col("COD_EDAD_INT"))=== "100 o +", "100".toInt).otherwise(trim(col("COD_EDAD_INT")).cast(IntegerType)))
      .na.fill("DESCONOCIDO", Array("DESC_BARRIO"))
      .na.fill(0)

    /*6.3)
    Enumera todos los barrios diferentes.*/
    df.select("DESC_BARRIO").distinct().show()

    /*6.4)
    Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios
    diferentes que hay.*/
    df.createOrReplaceTempView("padron")
    spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) FROM padron").show()

    /*6.5)
    Crea una nueva columna que muestre la longitud de los campos de la columna
    DESC_DISTRITO y que se llame "longitud"*/
    val df2 = df.withColumn("longitud", length(col("DESC_DISTRITO")))
    df2.show()

    /*6.6)
    Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla. */
    val df3 = df2.withColumn("valor", expr("5"))
    df3.show()

    /* 6.7)
    Borra esta columna.*/
    val df4 = df3.drop("valor")
    df4.show()

    /* 6.8)
    Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.*/
    val df5 = df4.repartition(col("DESC_DISTRITO"), col("DESC_BARRIO"))

    /*6.9)
    Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado
    de los rdds almacenados.*/
    df5.cache()

    /*6.10)
    Lanza una consulta contra el DF resultante en la que muestre el número total de
    "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres"
    para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en
    aparecer en el show. Los resultados deben estar ordenados en orden de más a menos
    según la columna "extranjerosmujeres" y desempatarán por la columna
    "extranjeroshombres"*/
    df5.groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .sum("ESPANOLESHOMBRES", "ESPANOLESMUJERES", "EXTRANJEROSHOMBRES", "EXTRANJEROSMUJERES")
      .orderBy(desc("sum(EXTRANJEROSMUJERES)"), desc("sum(EXTRANJEROSHOMBRES)"))
      .show()

    /*6.11)
    Elimina el registro en caché.*/
    df5.unpersist()

    /* 6.12)
    Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con
    DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres"
    residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a
    través de las columnas en común.*/
    val newdf = df.groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .sum("ESPANOLESHOMBRES")
      .orderBy()
    newdf.join(df, newdf("DESC_DISTRITO") === df("DESC_DISTRITO") and newdf("DESC_BARRIO") === df("DESC_BARRIO"), "inner").show()

    /*6.13)
    Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....))*/

    newdf
      .join(df, newdf("DESC_DISTRITO") === df("DESC_DISTRITO") and newdf("DESC_BARRIO") === df("DESC_BARRIO"), "inner")
      .withColumn("RowNumber", row_number().over(Window.partitionBy(newdf("DESC_BARRIO"), newdf("DESC_DISTRITO")).orderBy(newdf("DESC_BARRIO"), newdf("DESC_DISTRITO"))))
      .show()

    /* 6.14)
    Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que
    contenga los valores totales (la suma de valores) de espanolesmujeres para cada distrito y
    en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente
    CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a
    este:*/
    val a = df.where(col("DESC_DISTRITO") isin("BARAJAS", "CENTRO", "RETIRO"))
      .groupBy("COD_EDAD_INT")
      .pivot("DESC_DISTRITO")
      .sum("ESPANOLESMUJERES")
      .orderBy("COD_EDAD_INT")
    a.show()

    /*6.15)
Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje
de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa
cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la
condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.*/

    val b = a.withColumn("PORCENTAJE_BARAJAS",round(col("BARAJAS") * 100 / (col("BARAJAS") + col("CENTRO") + col("RETIRO")), 2))
      .withColumn("PORCENTAJE_CENTRO",round(col("CENTRO") * 100 / (col("BARAJAS") + col("CENTRO") + col("RETIRO")), 2))
      .withColumn("PORCENTAJE_RETIRO",round(col("RETIRO") * 100 / (col("BARAJAS") + col("CENTRO") + col("RETIRO")), 2))

    b.show()

    /* 6.16)
    Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un
    directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba
    que es la esperada*/
    df5.write.format("csv").mode("overwrite").save("src/main/resources/padron/csv/")

    /* 6.17)
    Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el
    resultado anterior.*/
    df5.write.format("parquet").mode("overwrite").save("src/main/resources/padron/parquet/")
  }
}
