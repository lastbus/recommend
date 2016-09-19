package com.bl.bigdata

import java.util.Properties

import com.bl.bigdata.mail.Message
import com.bl.bigdata.similarity.SeeBuyGoodsSimilarity
import com.bl.bigdata.util.{MyCommandLine, SparkFactory}
import org.apache.logging.log4j.LogManager

import scala.collection.mutable
import scala.xml.XML

/**
 * program entry point
 * Created by MK33 on 2016/6/30.
 */
object SparkDriver {

  val logger = LogManager.getLogger(this.getClass.getName)

  def main(args: Array[String]) {

    val optionMap = SparkDriverConf.parse(args)

    // load jobs
    val jobDescriptions = parseJobDescription("classes.xml")
    if (jobDescriptions.isEmpty) {
      System.err.println("No job found in classes.xml")
      sys.exit(-1)
    }
    val classMap = mutable.Map[String, JobDescription]()
    for (job <- jobDescriptions) classMap(job.shortName) = job

    if (optionMap.contains(SparkDriverConf.print) ) {
      println("\n jobs : ")
      for (c <- classMap) {
        println(c._1 + " : " + c._2)
      }
      sys.exit(0)
    }
    val executeMap = mutable.Map[String, JobDescription]()
    if (optionMap.contains(SparkDriverConf.all)) {
      if (optionMap.contains(SparkDriverConf.include)) {
        System.err.println(s"cannot include ${SparkDriverConf.all} and ${SparkDriverConf.include} meantime.")
        SparkDriverConf.printHelper
        sys.exit(-1)
      }
      classMap.foreach { case (k, v) => executeMap(k) = v }
    }
    if (optionMap.contains(SparkDriverConf.include)) {
       optionMap(SparkDriverConf.include).split(",").foreach { item =>
        if (classMap.contains(item)) {
          executeMap(item) = classMap(item)
        } else {
          System.err.println(s"not known class name: $item.")
          sys.exit(-1)
        }
      }
    }

    if (optionMap.contains(SparkDriverConf.exclude)) {
      optionMap(SparkDriverConf.exclude).split(",").foreach { item =>
        if (executeMap.contains(item)) {
          executeMap -= item
        } else if (!classMap.contains(item)) {
          System.err.println(s"not known excluded class name: $item.")
          sys.exit(-1)
        }
      }
    }
    if (executeMap.size == 0) {
      logger.info("Please input jobs to run: ")
      SparkDriverConf.printHelper
      sys.exit(-1)
    }
    logger.info("\n\n\n================   jobs to be executed   ======================")
    executeMap.foreach { case (k, v) => println("\t\t" + v) }
    println("\n\n\n")
    executeMap.foreach { case (shortName, job) =>
      try {
        val clazz = Class.forName(job.fullName)
        clazz.getMethod("run", classOf[Array[String]]).invoke(clazz.newInstance(), job.args.asInstanceOf[Object])
      } catch {
        case e: Exception =>
          logger.error("error: " + job.shortName + ": " + e)
          Thread.sleep(1000)
      }
        Thread.sleep(500)
    }

    logger.info("==============   #END#   ==================")
    if (optionMap.contains(SparkDriverConf.mail)) Message.sendMail
    // destroy resources
    SparkFactory.destroyResource()

  }

  def loadProperties(path: String): Properties = {
    val propsStream = this.getClass.getClassLoader.getResourceAsStream(path)
    if (propsStream != null) {
      try {
        val properties = new Properties()
        properties.load(propsStream)
        return properties
      } finally {
        propsStream.close()
      }
    }
    null
  }

  def parseJobDescription(path: String): Array[JobDescription] = {
    val xml = XML.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(path))
    val root = xml \ "class"
    val jobs = new Array[JobDescription](root.length)
    val  iterator = root.iterator
    var i = 0
    while (iterator.hasNext)
    {
      val node = iterator.next()
      jobs(i) = JobDescription((node \ "short-name").text, (node \ "full-name").text, (node \ "args").text.split(" "), (node \ "description").text)
      i += 1
    }
    jobs
  }


}

object SparkDriverConf {

  val include = "include"
  val exclude = "exclude"
  val all = "all"
  val print = "print"
  val mail = "mail"

  val sparkDriver = new MyCommandLine("SparkDriver")
  sparkDriver.addOption("i", include, true, "jobs to be executed")
  sparkDriver.addOption("e", exclude, true, "exclude jobs to be executed")
  sparkDriver.addOption("a", all, false, "execute all jobs in conf file")
  sparkDriver.addOption("p", print, false, "print all jobs")
  sparkDriver.addOption("m", mail, false, "send mail")

  def parse(args: Array[String]): Map[String, String] ={
    sparkDriver.parser(args)
  }

  def printHelper = sparkDriver.printHelper

}


case class JobDescription(shortName: String, fullName: String, args: Array[String] = Array[String](), description: String = ""){

  override def toString() = {
    s"""
      |short-name: $shortName, full-name: $fullName,  args: ${args.mkString(" ")}, description: $description
      """.stripMargin
  }
}
