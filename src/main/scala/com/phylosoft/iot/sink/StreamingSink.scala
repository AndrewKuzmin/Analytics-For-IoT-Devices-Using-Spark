package com.phylosoft.iot.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

trait StreamingSink {

  def start(outputDF: DataFrame): StreamingQuery

}
