package bl.testing.mess.logging

import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/3/23.
  */
object LogTtest {

  // 如果程序发生异常，则如何将详细的堆栈信息输出。
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
