package org.ejercicios.chapter3

import org.apache.spark.sql.types._
import org.compartida.Session

object Ej2 {
 def e {
   val spark = Session.s()
   val jsonFile = "src/main/resources/blogs.json"

   val schema = StructType(Array(StructField("Id", IntegerType, false),
     StructField("First", StringType, false),
     StructField("Last", StringType, false),
     StructField("Url", StringType, false),
     StructField("Published", StringType, false),
     StructField("Hits", IntegerType, false),
     StructField("Campaigns", ArrayType(StringType), false)))


   val blogsDF = spark.read.schema(schema).json(jsonFile)

   blogsDF.show(false)

   println(blogsDF.printSchema)
   println(blogsDF.schema)
 }
}
