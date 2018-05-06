package com.phylosoft.iot.source.kafka.json

import java.util.Properties

import com.phylosoft.iot.data.JsonSchemas
import com.phylosoft.iot.source.kafka.KafkaSource
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaRawDataJsonSource(val spark: SparkSession,
                             val properties: Properties)
  extends KafkaSource {

  override def getFullPlainInputDraftDF(rawInputDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val parsedDF = rawInputDF
      .select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), JsonSchemas.NEST_SCHEMA).alias("parsed_value")
      )
      .drop("key")

    val explodedInputdDF = parsedDF
      .select(lower($"parsed_value.shop").as("shop"),
        $"timestamp",
        explode($"parsed_value.devices"))
      .withColumnRenamed("key", "device")

    explodedInputdDF.printSchema()

    val plainInputDraftDF = explodedInputdDF
      .select($"*")
      .withColumn("timestamp", $"value".cast(TimestampType))
      .drop("value")

    plainInputDraftDF.printSchema()

    plainInputDraftDF

  }

}
