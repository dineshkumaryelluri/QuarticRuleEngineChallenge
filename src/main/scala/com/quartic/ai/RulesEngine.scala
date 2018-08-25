package com.quartic.ai

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, lit, when}

object RulesEngine {
  def applyRules(dataFrame: DataFrame, rulesMap: Map[String, Map[String, String]]) = {
    val dataset = dataFrame.withColumn("isViolated", lit(false))

    rulesMap.keys.foldLeft(dataset) {
      (df, rule) => {

        val comparisonType = rulesMap(rule)("comparisonType")
        val query = if (comparisonType == "Datetime")
          s"to_date(value,'yyyy-MM-dd HH:mm:SS') %s".format(rulesMap(rule)("condition"))
        else
          s"CAST(value AS %s) %s".format(comparisonType, rulesMap(rule)("condition"))


        df.withColumn("isViolated", when(col("signal") isin (rulesMap(rule)("signal").split(",", -1): _*)
          and (col("value_type").eqNullSafe(rulesMap(rule)("comparisonType"))),
          expr(query)).otherwise(col("isViolated")))
      }
    }
  }

  def applyRules(record: DataRecord, rulesMap: Map[String, Map[String, String]]): DataRecord = {
    val rules = rulesMap.keys.filter(key => rulesMap(key)("signal").split(",").contains(record.signal))

    rules.map(key => {
      val signal = record.signal
      val value_type = record.valueType
      val value = value_type match {
        case "Integer" => record.value.asInstanceOf[Int]
        case "Datetime" => {
          val format = new SimpleDateFormat("dd-MM-yyyy HH:mm:SS")
          format.parse(record.value.asInstanceOf[String])
        }
        case _ => record.value.asInstanceOf[String]
      }
      val isViolated = rulesMap(key)("condition").trim.charAt(0) match {
        case '>' => if (value.isInstanceOf[Date]) {
          value.asInstanceOf[Date].after(new Date())
        } else {
          value.asInstanceOf[Int] > rulesMap(key)("condition").trim.tail.toInt
        }
        case '<' => if (value.isInstanceOf[Date]) {
          value.asInstanceOf[Date].before(new Date())
        } else {
          value.asInstanceOf[Int] < rulesMap(key)("condition").trim.tail.toInt
        }
        case '=' => if (value.isInstanceOf[Date]) {
          value.asInstanceOf[Date] == new Date()
        } else if (value.isInstanceOf[Int]) {
          value.asInstanceOf[Int] < rulesMap(key)("condition").trim.tail.toInt
        } else {
          value.asInstanceOf[String] == rulesMap(key)("condition").trim.tail.replace("'", "")
        }
      }
      record.isViolated = isViolated
    })
    record
  }
}
