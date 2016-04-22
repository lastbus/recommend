package com.bl.bigdata.util

import org.apache.logging.log4j.LogManager

/**
 * Created by MK33 on 2016/4/22.
 */
trait Logging {
  val logger = LogManager.getLogger("RollingFile")
}
