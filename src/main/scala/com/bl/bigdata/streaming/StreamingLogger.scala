package com.bl.bigdata.streaming

import org.apache.logging.log4j.LogManager

/**
 * Created by MK33 on 2016/5/25.
 */
trait StreamingLogger {
  val logger = LogManager.getLogger("spark.streaming.kafka")
}
