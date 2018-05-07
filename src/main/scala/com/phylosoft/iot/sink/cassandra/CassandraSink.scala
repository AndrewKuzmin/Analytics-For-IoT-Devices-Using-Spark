package com.phylosoft.iot.sink.cassandra

import com.phylosoft.iot.sink.{Sink, StreamingSink}
import com.phylosoft.iot.utils.Provider
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SaveMode}

abstract class CassandraSink {

  private val appConf = Provider.getConfig

  def source: String = "org.apache.spark.sql.cassandra"

  def options: Map[String, String] = Map(
    "table" -> appConf.getString("cassandra.table"),
    "keyspace" -> appConf.getString("cassandra.keyspace"))

}

class CassandraNonStreamingSink extends CassandraSink with Sink {

  override def start(outputDF: DataFrame) {
    outputDF.write
      .format(source)
      .options(options)
      .mode(SaveMode.Append)
      .save()
  }
}

class CassandraStreamingSink extends CassandraSink with StreamingSink {

  override def start(outputDF: DataFrame): StreamingQuery = {
    outputDF.writeStream
      .format(source)
      .options(options)
      .outputMode(OutputMode.Append())
      .start()
  }

}

