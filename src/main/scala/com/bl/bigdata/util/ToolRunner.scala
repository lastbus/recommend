package com.bl.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/3/10.
  */
trait ToolRunner extends Tool {
  @transient private val logger = LogManager.getLogger(this.getClass.getName)

  abstract override def run(args: Array[String]): Unit = {
    val start = new Date
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    logger.info(s"task begins at: ${sdf.format(start)}")
    ConfigurationBL.init()
    super.run(args)
    val end = new Date
    logger.info(s"task ends at: ${sdf.format(end)}\n  time taken: ${(end.getTime - start.getTime) / 1000} s ")
  }

}
