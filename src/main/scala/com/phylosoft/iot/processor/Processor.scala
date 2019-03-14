package com.phylosoft.iot.processor

import com.phylosoft.iot.monitoring.Monitoring
import com.phylosoft.iot.sink.StreamingSink
import com.phylosoft.iot.source.StreamingSource
import com.phylosoft.iot.transform.NestTransformer
import com.phylosoft.iot.{Logger, Params, SparkSessionConfiguration}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

abstract class Processor(appName: String, params: Params)
  extends SparkSessionConfiguration
    with NestTransformer
    with Monitoring
    with Logger {

  private val appConf = ConfigFactory.load

  val settings = Map("spark.app.name" -> s"$appName with $params",
    "spark.sql.shuffle.partitions" -> "4"
  )

  def start(): Unit = {

    val inputSource = source

    val inputDF = inputSource.readStream

    val outputDF = transform(inputDF)

    val outputSink = sink

    val query = outputSink.writeStream(data = outputDF, getTriggerPolicy, getOutputMode)

    query.awaitTermination()

  }

  def source: StreamingSource = ???

  def sink: StreamingSink = ???

  def getTriggerPolicy: Trigger = Trigger.Once()

  def getOutputMode: OutputMode = OutputMode.Append()

  def transform(inputDF: DataFrame): DataFrame = {
    debug(inputDF)
    val result = process(inputDF)
    debug(result)
    result
  }

  def debug(df : DataFrame): Unit = {
    df.printSchema()
//    df.show(1)
//    println("Count = " + df.count())
  }

}
