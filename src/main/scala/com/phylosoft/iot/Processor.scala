package com.phylosoft.iot

import com.phylosoft.iot.source.file.JsonSource
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    val source = new JsonSource(spark)

    val joineDFs = source.getJsonStreamingInputDF

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
