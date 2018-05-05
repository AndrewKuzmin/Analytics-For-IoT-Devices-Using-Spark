package com.phylosoft.iot.devices

class DeviceAlerts {

  private def sendTwilio(message: String): Unit = {
    println("Twilio:" + message)
  }

  private def sendSNMP(message: String): Unit = {
    println("SNMP:" + message)
  }

  private def sendPOST(message: String): Unit = {
    println("HTTP POST:" + message)
  }

  private def publishOnConcluent(message: String): Unit = {
    println("Kafka Topic 'DeviceAlerts':" + message)
  }

  private def publishOnPubNub(message: String): Unit = {
    println("PubNub Channel 'DeviceAlerts':" + message)
  }

  def alert(args: Array[String]): Unit = {

    for (arg <- args) {
      val func = arg match {
        case "twilio" => sendTwilio _
        case "snmp" => sendSNMP _
        case "post" => sendPOST _
        case "kafka" => publishOnConcluent _
        case "pubnub" => publishOnPubNub _
      }
      func("Device 42 down!")
    }
  }

}
