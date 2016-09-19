package com.bl.bigdata.util

import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/7/13.
  */
trait Logging {
  val logger = LogManager.getLogger(this.getClass.getName)
}
