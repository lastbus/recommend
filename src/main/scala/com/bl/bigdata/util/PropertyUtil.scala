package com.bl.bigdata.util

import java.util.Properties
import org.apache.logging.log4j.LogManager
import scala.io.Source

/**
 * Created by blemall on 3/25/16.
 */
object PropertyUtil {

  private val logger = LogManager.getLogger(this.getClass)

  def get(key: String): String = {
      val properties = loadProperties("/home/blemall/workspace/config.properties")
      if (properties == null || properties.isEmpty)
        null
      else
        properties.getProperty(key)
  }

  private def loadProperties(filename: String): Properties = {
    val properties = new Properties
    try {
      using(Source.fromFile(filename)){ r =>
        val reader = r.bufferedReader()
        properties.load(reader)
      }
    } catch {
      case e: Exception => logger.info(e.getLocalizedMessage)
                           None
    }

    properties
  }

  private def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      if (resource != null)
        resource.close()
    }
  }
}
