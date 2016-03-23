package com.bl.bigdata.logging

import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/3/23.
  */
object LogTtest {

  private[this] final val logger = LogManager.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    logger.trace("trace")
    logger.info("info")
    logger.debug("debug")
    logger.warn("warn")
    logger.error("error")
    logger.fatal("fatal")
  }
}
