package com.phylosoft.iot.source

import org.apache.spark.sql.DataFrame

trait Source {

  def read: DataFrame

}
