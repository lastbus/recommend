package com.bl.bigdata.mail

/**
  * Created by MK33 on 2016/4/8.
  */
object Message {

  val message = new StringBuilder

  def sendMail = {
    MailServer.send(message.toString())
  }

  def addMessage(msg: String): Unit = {
    message.append(msg)
    message.append("\n")
  }

  def clear = message.clear()

}
