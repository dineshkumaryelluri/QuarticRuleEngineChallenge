package com.quartic.ai

import com.google.gson.Gson
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object StreamingReader extends App {
  val spark = SparkProvider.streamingContext(10)
  val lines = spark.socketTextStream("localhost", 9999)
  val config = ConfigReader.getConfig("config.conf")
  val rulesMap = ConfigReader.configAsMap(config)

  run(lines)

  def run(lines: ReceiverInputDStream[String]): Unit ={
    val dataRecords = lines.map(data => (new Gson).fromJson(data, DataRecord.getClass)).map(data => RulesEngine.applyRules(data.asInstanceOf[DataRecord], rulesMap))
    dataRecords.filter(!_.isViolated).saveAsTextFiles("")
  }
}

case class DataRecord(signal: String, value: Any, valueType: String, var isViolated: Boolean = false)