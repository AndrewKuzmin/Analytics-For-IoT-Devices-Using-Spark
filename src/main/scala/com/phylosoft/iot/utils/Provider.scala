package com.phylosoft.iot.utils

import com.typesafe.config.Config

object Provider {

  import com.typesafe.config.ConfigFactory

  private val config = ConfigFactory.load

  def getConfig: Config = config

}
