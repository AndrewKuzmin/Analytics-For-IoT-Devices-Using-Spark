package com.phylosoft.iot

import java.util.Properties

import com.phylosoft.iot.AppConfig.Mode
import com.phylosoft.iot.processor.Processor
import com.phylosoft.iot.sink.StreamingSink
import com.phylosoft.iot.sink.cassandra.CassandraSink
import com.phylosoft.iot.sink.console.ConsoleSink
import com.phylosoft.iot.source.StreamingSource
import com.phylosoft.iot.source.file.JsonSource
import com.phylosoft.iot.source.kafka.json.KafkaRawDataJsonSource
import com.phylosoft.iot.utils.Provider
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

    val appConf = Provider.getConfig

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    //        Logger.getRootLogger.setLevel(Level.WARN)

    val processor = params.mode match {
      case Mode.BATCH =>
        new Processor("MainApp", params) {
          override def source: StreamingSource = {
            new JsonSource(spark)
          }

          override def sink: StreamingSink = {
            new ConsoleSink
          }
        }
      case Mode.REALTIME =>
        new Processor("MainApp", params) {
          override def source: StreamingSource = {
            val properties = new Properties()
            properties.setProperty("subscribe", appConf.getString("kafka.topics.nest-json-raw-data"))
            new KafkaRawDataJsonSource(spark, properties)
          }

          override def sink: StreamingSink = {
            new CassandraSink
          }
        }
      case _ => sys.exit(1)
    }

    processor.start()

  }

}
