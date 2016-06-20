package com.bl.bigdata.util

import org.apache.log4j.{Level, Logger}

/**
 * Created by MK33 on 2016/5/29.
 */
trait LoggerShutDown {
  Logger.getRootLogger.setLevel(Level.WARN)
}
