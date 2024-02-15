package org.grupo1.ejercicios.chapter7

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.grupo1.compartida.Session

import scala.util.Random

object Ej2_SMJ {

  def e {
    val spark = Session.s()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new Random(42)

    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")

    val usersDF = spark.createDataFrame((0 to 1000000)
        .map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5)))))
      .toDF("uid", "login", "email", "user_state")

    val ordersDF = spark.createDataFrame((0 to 1000000)
        .map(r => (r, r, rnd.nextInt(10000), 10 * r* 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5)))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    val usersOrdersDF = ordersDF.join(usersDF, col("users_id") === col("uid"))

    usersOrdersDF.show(false)

    usersOrdersDF.explain()

    usersDF.orderBy(asc("uid"))
      .write.format("parquet")
      .bucketBy(8, "uid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("UsersTbl")

    ordersDF.orderBy(asc("users_id"))
      .write.format("parquet")
      .bucketBy(8, "users_id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("OrdersTbl")

    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")

    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    val joinUsersOrdersBucketDF = ordersBucketDF.join(usersBucketDF, col("users_id") === col("uid"))

    joinUsersOrdersBucketDF.show(false)

    joinUsersOrdersBucketDF.explain()

  }
}
