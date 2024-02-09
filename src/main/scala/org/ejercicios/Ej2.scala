package org.ejercicios

import org.compartida.Session
import org.apache.spark.sql.types._

object Ej2 {
 def e(s: Session) {
   val jsonFile = "src/main/resources/blogs.json"

   val schema = StructType(Array(StructField("Id", IntegerType, false),
     StructField("First", StringType, false),
     StructField("Last", StringType, false),
     StructField("Url", StringType, false),
     StructField("Published", StringType, false),
     StructField("Hits", IntegerType, false),
     StructField("Campaigns", ArrayType(StringType), false)))


   val blogsDF = s.spark.read.schema(schema).json(jsonFile)

   blogsDF.show(false)

   println(blogsDF.printSchema)
   println(blogsDF.schema)
 }
}
