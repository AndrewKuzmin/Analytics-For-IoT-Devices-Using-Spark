package com.phylosoft.iot.source.kafka.json

import java.util.Properties

import com.phylosoft.iot.source.kafka.KafkaSource
import org.apache.spark.sql.SparkSession

class KafkaJsonSource(val spark: SparkSession, val properties: Properties) extends KafkaSource {

}
