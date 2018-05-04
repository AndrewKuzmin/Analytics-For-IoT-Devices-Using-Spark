package com.phylosoft.iot

object MainApp {

  def main(args: Array[String]): Unit = {

    val processor = new Processor("MainApp")

    processor.start()

  }

}
