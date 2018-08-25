package com.quartic.ai

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._


object SparkProvider {
  val spark = SparkSession.builder().master("local").appName("ruleEngine").getOrCreate()
  val sparkContext = spark.sparkContext
  def streamingContext(batchInterval:Int) = new StreamingContext(sparkContext,Seconds(batchInterval))

}
