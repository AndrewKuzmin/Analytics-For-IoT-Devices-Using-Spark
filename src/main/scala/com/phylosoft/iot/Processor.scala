package com.phylosoft.iot

import com.phylosoft.iot.data.JsonSchemas
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class Processor(appName: String) {

  private val appConf = ConfigFactory.load

  private[iot] val spark = SparkSession
    .builder()
    .appName(appName)
    //      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      sparkConf.set("spark.kryoserializer.buffer", "24")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.cbo.enabled", "true")
    .getOrCreate()

  def start(): Unit = {

    val inputDF = spark.read
      .schema(JsonSchemas.NEST_SCHEMA)
      .option("multiLine", "true")
      .json("data/nest.json")
      .cache()
//    debug(inputDF)

    import spark.implicits._

//    val stringJsonDF = inputDF.select(to_json(struct($"*"))).toDF("nestDevice")
//    debug(stringJsonDF)

    val mapColumnsDF = inputDF.select(
      $"devices".getItem("smoke_co_alarms").alias("smoke_alarms"),
      $"devices".getItem("cameras").alias("cameras"),
      $"devices".getItem("thermostats").alias("thermostats"))
//    debug(mapColumnsDF)

    val explodedThermostatsDF = mapColumnsDF.select(explode($"thermostats"))
    val explodedCamerasDF = mapColumnsDF.select(explode($"cameras"))
    //or you could use the original nestDF2 and use the devices.X notation
    val explodedSmokedAlarmsDF = inputDF.select(explode($"devices.smoke_co_alarms"))

    val thermostateDF = explodedThermostatsDF.select($"value".getItem("device_id").alias("device_id"),
      $"value".getItem("locale").alias("locale"),
      $"value".getItem("where_name").alias("location"),
      $"value".getItem("last_connection").alias("last_connected"),
      $"value".getItem("humidity").alias("humidity"),
      $"value".getItem("target_temperature_f").alias("target_temperature_f"),
      $"value".getItem("hvac_mode").alias("mode"),
      $"value".getItem("software_version").alias("version"))

    val cameraDF = explodedCamerasDF.select($"value".getItem("device_id").alias("device_id"),
      $"value".getItem("where_name").alias("location"),
      $"value".getItem("software_version").alias("version"),
      $"value".getItem("activity_zones").getItem("name").alias("name"),
      $"value".getItem("activity_zones").getItem("id").alias("id"))

    val smokedAlarmsDF = explodedSmokedAlarmsDF.select($"value".getItem("device_id").alias("device_id"),
      $"value".getItem("where_name").alias("location"),
      $"value".getItem("software_version").alias("version"),
      $"value".getItem("last_connection").alias("last_connected"),
      $"value".getItem("battery_health").alias("battery_health"))

    val joineDFs = thermostateDF.join(cameraDF, "version")
    debug(joineDFs)

    val outputDF = checkAndFormatFromFile(joineDFs)

    outputDF.write
      .format("console")
      .save()

  }

  def checkAndFormatFromFile(inputDF: DataFrame): DataFrame = {
    inputDF
  }

  def debug(df : DataFrame): Unit = {
    df.printSchema()
    df.show(1)
    println("Count = " + df.count())
  }

}
