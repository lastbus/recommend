package com.bl.bigdata.util

import org.apache.log4j.{Level, Logger}

/**
 * Created by MK33 on 2016/5/29.
 */
trait LoggerShutDown {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("Remoting").setLevel(Level.WARN)

}
