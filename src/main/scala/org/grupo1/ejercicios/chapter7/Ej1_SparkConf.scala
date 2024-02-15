package org.grupo1.ejercicios.chapter7

import org.apache.spark.sql.SparkSession

object Ej1_SparkConf {

  def printConfigs(session: SparkSession) = {
    // Get conf
    val mconf = session.conf.getAll
    // Print them
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }

  def e {
    val spark = SparkSession.builder()
      .master("local")
      .appName("project1")
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    spark.sparkContext
      .setLogLevel("ERROR")

    printConfigs(spark)
    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark)

    spark.sql("SET -v").select("key", "value").show(5, false)
  }
}
