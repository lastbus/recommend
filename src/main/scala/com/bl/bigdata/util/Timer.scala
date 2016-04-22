package com.bl.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message

/**
 * Created by MK33 on 2016/4/22.
 */
trait Timer extends Tool {
  val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
  abstract override def run(args: Array[String]): Unit = {
    Message.clear
    Message.addMessage("====================   program begins   ======================")
    val begin = new Date()
    Message.addMessage(sdf.format(begin))
    Message.addMessage("\n")
    super.run(args)
    val ends = (System.currentTimeMillis() - begin.getTime) / 1000
    Message.addMessage(s"\ntime taken:\t$ends")
    Message.addMessage("====================   program ends      ======================")
    Message.sendMail
  }
}
