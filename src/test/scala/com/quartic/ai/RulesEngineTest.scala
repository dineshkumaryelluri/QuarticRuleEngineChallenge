package com.quartic.ai

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RulesEngineTest extends FunSuite with DataFrameSuiteBase {

  test("test apply rules with DataFrame") {
    val rulesMap = Map("rule1" -> Map("signal" -> "ATL1", "comparisonTye" -> "Integer", "condition" -> ">240"))
    System.setProperty("HADOOP_HOME","D:\\winutils-master\\hadoop-2.7.1")
    import spark.implicits._
    val dataframe: DataFrame = sc.parallelize(List(List("ATL1", "250", "Integer"), List("ATL1", "HIGH", "String"))).toDF("signal", "value", "value_type")
    val df = RulesEngine.applyRules(dataframe, rulesMap)
    val expectedDF = sc.parallelize(List(List("ATL1", "250", "Integer", true), List("ATL1", "HIGH", "String", false))).toDF("signal", "value", "value_type", "isViolated")
    assertDataFrameEquals(df, expectedDF)
  }

}
