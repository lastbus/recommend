package com.bl.bigdata.ranking

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import com.bl.bigdata.util.ConfigurationBL
import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/3/23.
  */
class HotSaleConf extends ConfigurationBL {

  private val logger = LogManager.getLogger(this.getClass)

  private val setting = new ConcurrentHashMap[String, String]()

  def this(file: String*) {
    this()
    for (f <- file) parseConfFile(f)
  }

  /** 解析配置文件 */
  def parseConfFile(file: String): Unit = {
    val kV = addResource(file)
    if (!kV.isEmpty)
      for ((key, value) <- kV) {
        if(setting.containsKey(key))
          logger.warn(s"$key's origin value ${setting.get(key)} is overriding by $value.")
        setting.put(key, value)
    }
  }

  /** Get a parameter, throws an exception if not found */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException)
  }
  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    if(setting.isEmpty) logger.warn("configuration is empty!")
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(setting.get(key))
  }

  def getAll: Array[(String, String)] = {
    setting.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }


}
