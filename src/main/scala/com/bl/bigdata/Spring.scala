package com.bl.bigdata


import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.{ComponentScan, Bean, AnnotationConfigApplicationContext, Configuration}
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.stereotype.Component

/**
  * Created by MK33 on 2016/3/18.
  */
@Configuration
@ComponentScan
@EnableScheduling
class Spring {

  @Bean
  def mockMessageService(): MessageService = {
    new MessageService {
      override def getMessage: String = "Hello World!"
    }
  }

  def run(args: Array[String]): Unit = {
    val context = new AnnotationConfigApplicationContext(classOf[Spring])
    val printer = context.getBean(classOf[MessagePrinter])
    printer.printMessage

  }

}

object Spring {
  def main(args: Array[String]) {
    (new Spring).run(args)
  }
}

trait MessageService {
  def getMessage(): String
}

@Component
class MessagePrinter {
  private var service: MessageService = null

  @Autowired def this(service: MessageService) {
    this()
    this.service = service
  }

  def printMessage {
    System.out.println(this.service.getMessage)
  }
}