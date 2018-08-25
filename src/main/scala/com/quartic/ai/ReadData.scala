package com.quartic.ai

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ReadData {
  def main(args: Array[String]): Unit = {
    val spark = SparkProvider.spark
    val df = spark.read.json("C:\\Users\\yelluri.kumar\\Desktop\\raw_data_out.json")
    val config = ConfigReader.getConfig("config.conf")
    val rulesMap = ConfigReader.configAsMap(config)
    val dataset = RulesEngine.applyRules(df, rulesMap)
    dataset.filter(col("isViolated")===true).show()
  }
}

