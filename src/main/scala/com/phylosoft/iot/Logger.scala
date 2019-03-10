package com.phylosoft.iot

import org.apache.log4j
import org.apache.log4j.{Level, LogManager}

/**
  * Created by Andrew Kuzmin on 3/10/2019.
  */
trait Logger {

  val log: log4j.Logger = LogManager.getRootLogger
  log.setLevel(Level.WARN)

  def log(msg: String): Unit = {
    println(msg)
  }

}