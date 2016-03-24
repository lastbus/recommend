package com.bl.bigdata.util

import org.apache.logging.log4j.LogManager

import scala.xml.XML

/**
 * Created by MK33 on 2016/3/23.
 */
trait ConfigurationBL extends ConfigurableBL {

  private val logger = LogManager.getLogger(this.getClass)

  def addResource(path: String): Traversable[(String, String)] ={
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
    keyValues
  }

}
