package com.phylosoft.iot.transform

import java.io.File

import com.phylosoft.iot.Params
import com.phylosoft.iot.sink.console.ConsoleSink
import com.phylosoft.iot.source.file.JsonSource
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Processor(appName: String, params: Params) {

  private val appConf = ConfigFactory.load

  // warehouseLocation points to the default location for managed databases and tables
  private val warehouseLocation = "file:///" + new File("spark-warehouse").getAbsolutePath.toString

  private val conf = new SparkConf()
    .setAppName(s"$appName with $params")
    .set("spark.sql.warehouse.dir", warehouseLocation)
    .set("spark.sql.shuffle.partitions", "4") // keep the size of shuffles small
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.kryoserializer.buffer", "24")
    .set("spark.sql.session.timeZone", "UTC")
    .set("spark.sql.cbo.enabled", "true")

  private[iot] val spark = SparkSession
    .builder
    .config(conf)
    //      .enableHiveSupport()
    .getOrCreate()

  def start(): Unit = {

    val inputSource = new JsonSource(spark)

    val joineDFs = inputSource.getJsonStreamingInputDF

    val outputDF = checkAndFormatFromFile(joineDFs)

    val outputSink = new ConsoleSink

    val query = outputSink.start(outputDF, Trigger.Once(), OutputMode.Append())

    query.awaitTermination()

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
