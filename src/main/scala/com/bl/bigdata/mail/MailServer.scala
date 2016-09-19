package com.bl.bigdata.mail

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.bl.bigdata.util.ConfigurationBL
import org.apache.commons.mail.{DefaultAuthenticator, SimpleEmail}
import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/3/28.
  */
object MailServer {
  private val logger = LogManager.getLogger(this.getClass.getName)

  lazy val mailProps: Properties = {
    val properties = new Properties()
    val in = Thread.currentThread().getContextClassLoader.getResourceAsStream("mail.properties")
    if (in == null) {
      println("no mail.properties found.")
      null
    } else {
      try {
        properties.load(in)
      } finally {
        in.close()
      }
      properties
    }
  }

  def send(message: String): Unit = {
    if (mailProps == null) {
      println("no mail properties file found, exit.")
      return
    }
    val email = getEmail()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    email.setMsg(message)
    email.setSubject("redis report: " + sdf.format(new Date))
    logger.info(s"send email: $message")
    email.send
  }

  def getEmail(): SimpleEmail = {
    val email = new SimpleEmail
    email.setHostName(mailProps.getProperty("mail.host"))
    email.setAuthenticator(new DefaultAuthenticator(mailProps.getProperty("mail.user"), mailProps.getProperty("mail.password")))
    email.setFrom(mailProps.getProperty("mail.from"))
    for (who <- mailProps.getProperty("mail.to").split(",")) email.addTo(who)
    email
  }

  def main(args: Array[String]) {

    send("test")

  }


  def test = {
    val email = new SimpleEmail
    email.setHostName("mail.bl.com")
    email.setDebug(true)
    email.setAuthenticator(new DefaultAuthenticator("RPublish", "bl@123"))
    email.setFrom("Recommend-Publish@bl.com")
    email.addTo("Ke.Ma@bl.com")
    email.send()

  }


}
