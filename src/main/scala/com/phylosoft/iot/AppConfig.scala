package com.phylosoft.iot

object AppConfig {

  object Mode extends Enumeration {
    type Mode = Value
    val REALTIME, BATCH = Value
  }

  object Source extends Enumeration {
    type Source = Value
    val KAFKA, JSON, AVRO = Value
  }

  object Sink extends Enumeration {
    type Sink = Value
    val KAFKA, JSON, AVRO = Value
  }

}
