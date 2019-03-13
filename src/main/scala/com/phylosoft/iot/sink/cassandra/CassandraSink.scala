package com.phylosoft.iot.sink.cassandra

import com.phylosoft.iot.sink.StreamingSink
import com.phylosoft.iot.utils.Provider
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

/**
  * Created by Andrew Kuzmin on 3/10/2019.
  */
class CassandraSink()
  extends StreamingSink {

  private val appConf = Provider.getConfig

  private val source: String = "org.apache.spark.sql.cassandra"

  private val options: Map[String, String] = Map(
    "table" -> appConf.getString("cassandra.tables.table_nest_thermostat"),
    "keyspace" -> appConf.getString("cassandra.keyspace"))


  def writeStream(data: DataFrame,
                  trigger: Trigger = Trigger.Once(),
                  outputMode: OutputMode = OutputMode.Append()): StreamingQuery = {
    data.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("org.apache.spark.sql.cassandra")
          .options(options)
          .mode(SaveMode.Append)
          .save()
      }
      //
      //      .format(source)
      //      .options(options)


      .outputMode(outputMode)
      .start()
  }

}
