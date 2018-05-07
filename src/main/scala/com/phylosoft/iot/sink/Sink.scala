package com.phylosoft.iot.sink

import org.apache.spark.sql.DataFrame

trait Sink {

  def start(outputDF: DataFrame)

}
