package com.phylosoft.iot.monitoring

import com.phylosoft.iot.SparkSessionConfiguration

trait Monitoring {

  this: SparkSessionConfiguration =>

  lazy val simpleListener = new SimpleListener

}
