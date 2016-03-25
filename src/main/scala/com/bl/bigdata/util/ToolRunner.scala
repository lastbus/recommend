package com.bl.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/3/10.
  */
trait ToolRunner extends Tool with Serializable {
  private val logger = LogManager.getLogger(this.getClass.getName)

  abstract override def run(args: Array[String]): Unit = {
    val start = new Date
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    logger.info(s"task begins at: ${sdf.format(start)}")
    try {
      super.run(args)
    } catch {
      case e: Exception =>
        logger.info(s"encounter error, program exit: ${e.getMessage}")
    }
    val end = new Date
    logger.info(s"task ends at: ${sdf.format(end)}\n  time taken: ${(end.getTime - start.getTime) / 1000} s ")
  }

}
