package com.bl.bigdata.util

import java.util.concurrent.ConcurrentHashMap
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import org.apache.logging.log4j.LogManager

import scala.xml.XML

/**
 * Created by MK33 on 2016/3/23.
 */
object ConfigurationBL extends ConfigurableBL {

  private val logger = LogManager.getLogger(this.getClass)
  private val setting = new ConcurrentHashMap[String, String]

  /** 解析配置文件 */
  def parseConfFile(file: String): Unit = {
//    val kV = addResource(file)
//    if (!kV.isEmpty)
//      for ((key, value) <- kV) {
//        if(setting.containsKey(key))
//          logger.warn(s"$key's origin value ${setting.get(key)} is overriding by $value.")
//        setting.put(key, value)
//      }
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(setting.get(key))
  }

  def getAll: Array[(String, String)] = {
    setting.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  def addResource(path: String) = {
    logger.info(s"begin to parse configuration file: $path.")
    val xml = XML.load(path)
    val properties = xml \ "property"
    val size = properties.length
    logger.debug(size)
    val keyValues = new Array[(String, String)](size)
    var i = 0
    for (property <- properties) {
      val name = property \ "name"
      val value = property \ "value"
      keyValues(i) = (name.text.trim, value.text.trim)
      logger.debug(name.text)
      i += 1
    }
    logger.info(s"parse finished, loaded ${size} properties.")
    if (!keyValues.isEmpty)
      for ((key, value) <- keyValues) {
        if(setting.containsKey(key))
          logger.warn(s"$key's origin value ${setting.get(key)} is overriding by $value.")
        setting.put(key, value)
      }

  }

}
