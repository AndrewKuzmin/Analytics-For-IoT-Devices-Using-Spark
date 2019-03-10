package com.phylosoft.iot.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Andrew Kuzmin on 3/10/2019.
  */
trait StreamingSource {

  def readStream: DataFrame

}
