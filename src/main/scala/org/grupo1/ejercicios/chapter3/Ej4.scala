package org.ejercicios.chapter3

import org.apache.spark.sql.functions._
import org.compartida.Session
import org.datasets._

object Ej4 {
  def e {
    val spark = Session.s()

    import spark.implicits._

    val ds = spark.read
      .json("src/main/resources/iot_devices.json")
      .as[DeviceIoTData]

    ds.show(5, false)

    val filterTempDS = ds.filter({d=>{d.temp > 30 && d.humidity > 70}})

    filterTempDS.show(5, false)

    val dsTemp = ds.filter(d => {d.temp > 25})
      .map(d=> (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]

    dsTemp.show(5, false)

    val device = dsTemp.first()
    println(device)

    val dsTemp2 = ds.select($"temp", $"device_name", $"device_id", $"cca3")
      .where("temp > 25")
      .as[DeviceTempByCountry]

    dsTemp2.show(5, false)

    ds.filter({d=> d.battery_level < 2}).show()

    ds.groupBy($"cn")
      .avg("c02_level")
      .orderBy(desc("avg(c02_level)"))
      .show()

    ds.select(max($"temp"), min($"temp"), max($"battery_level"), min($"battery_level"),
      max($"c02_level"), min($"c02_level"), max($"humidity"), min($"humidity"))
      .show()


  }
}
