package com.bl.bigdata.mail

/**
  * Created by MK33 on 2016/4/8.
  */
object Message {

  /** 邮件内容 */
  val message = new StringBuilder

  /**
    * 发送邮件
    */
  def sendMail = {
    MailServer.send(message.toString())
  }

  /**
    * 往邮件里面添加内容
    * @param msg
    */
  def addMessage(msg: String): Unit = {
    message.append(msg)
    message.append("\n")
  }

  /**
    * 清空邮件内容
    */
  def clear = message.clear()

}
