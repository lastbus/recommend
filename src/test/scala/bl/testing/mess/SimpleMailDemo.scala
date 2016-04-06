package bl.testing.mess

import org.apache.commons.mail.{DefaultAuthenticator, SimpleEmail}

/**
  * Created by MK33 on 2016/3/28.
  */
object SimpleMailDemo {

  def main(args: Array[String]) {
//    MailServer.send("test")
    blMail
  }

  def demo: Unit = {
    val email = new SimpleEmail()
    email.setHostName("smtp.sina.com")
    email.setAuthenticator(new DefaultAuthenticator("disanyuzhou2016@sina.com", "sh1@bl2$3"))
    email.setFrom("disanyuzhou2016@sina.com")
    email.setSubject("TestMail")
    email.setMsg("This is a test mail ... :-)")
    email.addTo("Ke.Ma@bl.com")
    email.send()
  }

  def blMail = {
    val email = new SimpleEmail
    email.setHostName("mail.bl.com")
    email.setAuthenticator(new DefaultAuthenticator("MK33", "Make819307659"))
    email.setFrom("Ke.Ma@bl.com")
    email.setMsg("This is a test mail....")
    email.addTo("Ke.Ma@bl.com")
    email.send()
  }
}
