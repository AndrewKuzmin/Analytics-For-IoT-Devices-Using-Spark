package com.phylosoft.iot.source.kafka

import java.util.Properties

import com.phylosoft.iot.utils.Provider
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Andrew Kuzmin on 3/10/2019.
  */
trait KafkaSource {

  val spark: SparkSession
  val properties: Properties

  def getStreamingInputDF: DataFrame = {

    val rawInputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Provider.getConfig.getString("kafka.bootstrap_servers")) // comma separated list of broker:host
      .option("subscribe", properties.getProperty("subscribe")) // comma separated list of topics
      //      .option("startingOffsets", "earliest") // read data from the start of the stream ("earliest" for batch)
      .option("startingOffsets", "latest") // read data from the end of the stream ("latest" for streaming)
      //      .option("endingOffsets", "latest") // batch query
      .option("minPartitions", "10")
      .option("failOnDataLoss", "false")
      .load()

    rawInputDF.printSchema()

    val fullPlainInputDraftDF = getFullPlainInputDraftDF(rawInputDF)

    fullPlainInputDraftDF

  }

  def getFullPlainInputDraftDF(rawInputDF: DataFrame): DataFrame = ???


}
