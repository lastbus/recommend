package com.bl.bigdata


import com.bl.bigdata.util._


/**
 * 主程序入口
  * Created by MK33 on 2016/3/30.
  */
object Main {

  def main(args: Array[String]) {
    ConfigurationBL.init()
    val toolManager = new ToolManager with Timer
    toolManager.run(args)
    SparkFactory.destroyResource()
  }

}

