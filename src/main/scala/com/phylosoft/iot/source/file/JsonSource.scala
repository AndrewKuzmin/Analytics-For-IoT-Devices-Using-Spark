package com.phylosoft.iot.source.file

import com.phylosoft.iot.data.JsonSchemas
import com.phylosoft.iot.source.StreamingSource
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonSource(val spark: SparkSession)
  extends StreamingSource {

  def readStream: DataFrame = {

    val inputDF = spark
      .readStream
      .schema(JsonSchemas.NEST_SCHEMA)
      .option("multiLine", "true")
      .json("data/nest/")
    //      .cache()
    //    debug(inputDF)

    inputDF

  }

}
