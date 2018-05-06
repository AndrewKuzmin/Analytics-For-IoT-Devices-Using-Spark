package com.phylosoft.iot.sink.foreach

import java.util.Properties

import com.phylosoft.iot.domain.Model.DeviceData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.ForeachWriter

class KafkaSink(kafkaProps: Properties) extends ForeachWriter[DeviceData] {

  // KafkaProducer can't be serialized, so we're creating it locally for each partition.
  var producer: KafkaProducer[String, DeviceData] = _

  override def open(partitionId: Long, version: Long): Boolean = {
    producer = new KafkaProducer[String, DeviceData](kafkaProps)
    true
  }

  override def process(value: DeviceData): Unit = {
    val message = new ProducerRecord[String, DeviceData]("topic", value.id, value)
    println("sending windowed message: " + value)
    producer.send(message)
  }

  override def close(errorOrNull: Throwable): Unit = {
    producer.close()
  }

}
