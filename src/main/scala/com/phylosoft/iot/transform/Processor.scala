package com.phylosoft.iot.transform

import java.util.Properties

import com.phylosoft.iot.monitoring.Monitoring
import com.phylosoft.iot.sink.console.ConsoleSink
import com.phylosoft.iot.source.file.JsonSource
import com.phylosoft.iot.source.kafka.json.KafkaRawDataJsonSource
import com.phylosoft.iot.{Logger, Params, SparkSessionConfiguration}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

class Processor(appName: String, params: Params)
  extends SparkSessionConfiguration
    with Monitoring
    with Logger {

  private val appConf = ConfigFactory.load

  val settings = Map("spark.app.name" -> s"$appName with $params",
    "spark.sql.shuffle.partitions" -> "4"
  )

  def start(): Unit = {

//    val inputSource = new JsonSource(spark)

    val properties = new Properties()
    properties.setProperty("subscribe", appConf.getString("kafka.topics.nest-json-raw-data"))
    val inputSource = new KafkaRawDataJsonSource(spark, properties)

    val inputDF = inputSource.readStream

    val outputDF = checkAndFormatFromFile(inputDF)

    val outputSink = new ConsoleSink

    val query = outputSink.writeStream(outputDF, Trigger.Once(), OutputMode.Append())

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
