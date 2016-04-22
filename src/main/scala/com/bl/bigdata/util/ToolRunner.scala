package com.bl.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message

/**
  * Created by MK33 on 2016/3/10.
  */
trait ToolRunner extends Tool {

  abstract override def run(args: Array[String]): Unit = {
    val start = new Date
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    Message.addMessage(s"\t=====   Task begins at: ${sdf.format(start)}  ======")
    super.run(args)
    val end = new Date
    Message.addMessage(s"\ttime taken: ${(end.getTime - start.getTime) / 1000} s")
    Message.addMessage(s"\t=====   Task ends at: ${sdf.format(end)}  ======")
  }

}
