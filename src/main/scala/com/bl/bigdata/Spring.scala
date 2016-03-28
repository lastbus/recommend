package com.bl.bigdata


import hello.{MessagePrinter, MessageService}
import org.springframework.context.annotation.{ComponentScan, Bean, AnnotationConfigApplicationContext, Configuration}

/**
  * Created by MK33 on 2016/3/18.
  */
@Configuration
@ComponentScan
object Spring {

  @Bean
  def mockMessageService(): MessageService = {
    new MessageService {
      override def getMessage: String = "Hello World!"
    }
  }

  def main(args: Array[String]): Unit = {
    val context = new AnnotationConfigApplicationContext(Spring.getClass)
    val printer = context.getBean(classOf[MessagePrinter])
    printer.printMessage()

  }
}
