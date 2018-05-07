package com.phylosoft.iot.sink.console

import java.io.File

import com.phylosoft.iot.sink.StreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

class ConsoleSink extends StreamingSink {

  def start(outputDF: DataFrame): StreamingQuery = {

    // Provider.getConfig.getString("spark.checkpoint_location_spark_first")
    val checkpointLocation = "file:///" + new File("checkpoint").getAbsolutePath + "/spark_first"

    outputDF
      .writeStream
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .start()
  }

}
