package com.phylosoft.iot.sink.kafka

import java.io.File
import java.util.Properties

import com.phylosoft.iot.Params
import com.phylosoft.iot.sink.StreamingSink
import com.phylosoft.iot.utils.Provider
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
  * Created by Andrew Kuzmin on 3/10/2019.
  */
class KafkaSink(private val spark: SparkSession,
                private val params: Params,
                private val kafkaProps: Properties)
  extends StreamingSink {

  def writeStream(data: DataFrame,
            trigger: Trigger = Trigger.Once(),
            outputMode: OutputMode = OutputMode.Update()): StreamingQuery = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dataTopicPrefix = Provider.getConfig.getString("kafka.topics.data_stream_prefix")

    //      Provider.getConfig.getString("spark.checkpoint_location_importer")
    val checkpointLocation = "file:///" + new File("checkpoint").getAbsolutePath + "/" + dataTopicPrefix

    val query = data
      .select($"*", concat_ws("-", lit(dataTopicPrefix), $"columnA").alias("topic"))
      .selectExpr("topic",
        "CAST(columnA AS STRING) AS key",
        "to_json(struct(columnA, columnB, columnC, columnD, columnE, timestamp)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Provider.getConfig.getString("kafka.bootstrap_servers"))
      //        .option("topic", topic)
      //        .trigger(Trigger.ProcessingTime(3.seconds))
      //        .outputMode(OutputMode.Update())
      .option("checkpointLocation", checkpointLocation)
      .start()

    query

  }

}