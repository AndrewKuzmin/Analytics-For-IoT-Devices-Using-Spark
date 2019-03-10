package com.phylosoft.iot

import com.phylosoft.iot.AppConfig.Mode
import com.phylosoft.iot.transform.Processor
import org.apache.log4j.{Level, LogManager}
import scopt.OptionParser

object MainApp {

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("MainApp") {
      head("MainApp: a stream analytics for IoT devices.")
      arg[String]("<mode>")
        .text("a mode of processing input data [realtime, batch]")
        .required()
        .action((x, c) => c.copy(mode = Mode.withName(x.toUpperCase)))
      arg[String]("<environment>")
        .text("a processing environment [dev, test, stage, prod]")
        .required()
        .action((x, c) => c.copy(environment = x.toUpperCase))

    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }


  }

  def run(params: Params): Unit = {

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    //        Logger.getRootLogger.setLevel(Level.WARN)

    val processor = new Processor("MainApp", params)

    processor.start()


  }

}
