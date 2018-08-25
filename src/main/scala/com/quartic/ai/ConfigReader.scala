package com.quartic.ai

import com.typesafe.config.{ConfigFactory, ConfigObject}

object ConfigReader {

  def getConfig(fileName: String): ConfigObject = {
    ConfigFactory.load(fileName).getObject("rules").toConfig.root()
  }

  def configAsMap(config: ConfigObject): Map[String, Map[String, String]] = {
    val keys: Array[String] = getKeySet(config)
    keys.map(key => (key, configAsMap(getSubConfig(config, key), getKeySet(getSubConfig(config, key))))).toMap
  }

  def configAsMap(config: ConfigObject, keySet: Array[String]) = {
    keySet.map(key => (key, getValue(config, key))).toMap
  }

  def getValue(configValue: ConfigObject, key: String) = configValue.get(key).render().replace("\"","")

  def getKeySet(configObject: ConfigObject): Array[String] = {
    configObject.keySet().toArray.map(_.asInstanceOf[String])
  }

  def getSubConfig(configObject: ConfigObject, key: String): ConfigObject = configObject.get(key).asInstanceOf[ConfigObject]
}
