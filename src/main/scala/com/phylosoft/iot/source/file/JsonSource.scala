package com.phylosoft.iot.source.file

import com.phylosoft.iot.data.JsonSchemas
import com.phylosoft.iot.source.StreamingSource
import com.phylosoft.iot.transform.NestTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonSource(val spark: SparkSession)
  extends StreamingSource
    with NestTransformer {

  def readStream: DataFrame = {

    val inputDF = spark
      .readStream
      .schema(JsonSchemas.NEST_SCHEMA)
      .option("multiLine", "true")
      .json("data/nest/")
    //      .cache()
    //    debug(inputDF)

    process(inputDF)

  }

}
