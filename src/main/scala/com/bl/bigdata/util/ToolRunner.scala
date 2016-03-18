package com.bl.bigdata.util

/**
  * Created by MK33 on 2016/3/10.
  */
object ToolRunner {

  def run(config: Configurable, tool: Tool, args: Array[String]): Unit ={
    val startTime = System.currentTimeMillis()

    try {

    } catch {
      case _: Exception => println(s"There are error when execute ${tool.getClass.getName}")
    }
    tool.run(args)


    val endTime = System.currentTimeMillis()
    println(s"time taken: ${(endTime - startTime) / 1000} s ")
  }

}
