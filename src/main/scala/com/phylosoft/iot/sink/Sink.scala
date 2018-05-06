package com.phylosoft.iot.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

trait Sink {

  def start(outputDF: DataFrame): StreamingQuery

}
