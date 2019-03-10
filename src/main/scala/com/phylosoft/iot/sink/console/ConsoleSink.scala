package com.phylosoft.iot.sink.console

import java.io.File

import com.phylosoft.iot.sink.StreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

class ConsoleSink()
  extends StreamingSink {

  override def start(data: DataFrame,
                     trigger: Trigger,
                     outputMode: OutputMode): StreamingQuery = {

    // Provider.getConfig.getString("spark.checkpoint_location_spark_first")
    val checkpointLocation = "file:///" + new File("checkpoint").getAbsolutePath + "/spark_first"

    data.writeStream
      .format("console")
      .trigger(trigger)
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointLocation)
      .start()
  }

}