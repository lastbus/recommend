package com.bl.bigdata


import com.bl.bigdata.util._


/**
 * 主程序入口
  * Created by MK33 on 2016/3/30.
  */
object Main {

  def main(args: Array[String]) {
    ConfigurationBL.init()
    val main = new Main with Timer
    main.run(args)
  }

}

class Main extends Tool {
  override def run(args: Array[String]): Unit = {
    val toolManager = new ToolManager(args)
    toolManager.start
  }
}
