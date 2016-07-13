package com.bl.bigdata

import java.util.Properties

import com.bl.bigdata.util.{ConfigurationBL, SparkFactory}
import org.apache.commons.cli._
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
    // load jobs
    val jobDescriptions = parseJobDescription("classes.xml")
    if (jobDescriptions.isEmpty) {
      System.err.println("No job found in classes.xml")
      sys.exit(-1)
    }
    val classMap = mutable.Map[String, JobDescription]()
    for (job <- jobDescriptions) classMap(job.shortName) = job
    val executeMap = mutable.Map[String, JobDescription]()
    // parse command line parameters
    SparkDriverCli.parse(args)
    if (SparkDriverConf.optionMap.contains(SparkDriverConf.all)) {
      if (SparkDriverConf.optionMap.contains(SparkDriverConf.include)) {
        System.err.println(s"cannot include ${SparkDriverConf.all} and ${SparkDriverConf.include} meantime.")
        SparkDriverCli.printHelper()
        sys.exit(-1)
      }
      classMap.foreach { case (k, v) => executeMap(k) = v }
      //exclude jobs
      if (SparkDriverConf.optionMap.contains(SparkDriverConf.exclude)) {
        SparkDriverConf.optionMap(SparkDriverConf.exclude).split(",").foreach { item =>
          if (executeMap.contains(item)) executeMap -= item
          else {
            System.err.println(s"not known class name: ${item}.")
            SparkDriverCli.printHelper()
            sys.exit(-1)
          }
        }
      }
    }
    if (SparkDriverConf.optionMap.contains(SparkDriverConf.include)) {
      SparkDriverConf.optionMap(SparkDriverConf.include).split(",").foreach { item =>
        if (classMap.contains(item)) {
          executeMap(item) = classMap(item)
        } else {
          System.err.println(s"not known class name: ${item}.")
          sys.exit(-1)
        }
      }
    }

    if (SparkDriverConf.optionMap.contains(SparkDriverConf.exclude)) {
      SparkDriverConf.optionMap(SparkDriverConf.exclude).split(",").foreach { item =>
        if (executeMap.contains(item)) {
          executeMap -= item
        } else if (!classMap.contains(item)) {
          System.err.println(s"not known excluded class name: ${item}.")
          sys.exit(-1)
        }
      }
    }
    if (executeMap.size == 0) {
      println("Please input jobs to run: ")
      SparkDriverCli.printHelper()
      sys.exit(-1)
    }
    logger.info("\n\n\n================   jobs to be executed   ======================")
    executeMap.foreach { case (k, v) => println("\t\t" + v) }
    println("\n\n\n")
//    ConfigurationBL.init()
    executeMap.foreach { case (shortName, job) =>
      try {
        val clazz = Class.forName(job.fullName)
//        clazz.getMethod("run", classOf[Array[String]]).invoke(clazz.newInstance(), job.args.asInstanceOf[Object])
      } catch {
        case e: Exception =>
          logger.error("error: " + job.shortName + ": " + e.getMessage)
      }
    }

    println("==============   #END#   ==================")
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

object SparkDriverCli {

  val options = new Options

  val help = new Option("h", "help", false, "print help information.")
  val include = new Option("i", SparkDriverConf.include, true, "jobs to run.")
  include.setArgName("className")
  val exclude = new Option("e", SparkDriverConf.exclude, true, "jobs not to run.")
  exclude.setArgName("className")
  val all = new Option("a", SparkDriverConf.all, false, "run all jobs in properties file not include exclude jobs.")

  options.addOption(help)
  options.addOption(include)
  options.addOption(exclude)
  options.addOption(all)

  val commandParser = new BasicParser

  def parse(args: Array[String]): Unit = {
    val commandLine = try {
      commandParser.parse(options, args)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        printHelper()
        sys.exit(-1)
    }

    if (commandLine.hasOption("help")) {
      printHelper()
      sys.exit(-1)
    }

    if (commandLine.hasOption(SparkDriverConf.all)){
      SparkDriverConf.optionMap(SparkDriverConf.all) = "true"
    }
    if (commandLine.hasOption(SparkDriverConf.exclude)) {
      SparkDriverConf.optionMap(SparkDriverConf.exclude) = commandLine.getOptionValue(SparkDriverConf.exclude)
    }
    if (commandLine.hasOption(SparkDriverConf.include)) {
      SparkDriverConf.optionMap(SparkDriverConf.include) = commandLine.getOptionValue(SparkDriverConf.include)
    }

  }

  def printHelper(): Unit = {
    val helperFormatter = new HelpFormatter
    helperFormatter.printHelp("SparkDriver", options)
  }

}

object SparkDriverConf {

  val include = "include"
  val exclude = "exclude"
  val all = "all"

  val optionMap = mutable.Map[String, String]()
}

case class JobDescription(shortName: String, fullName: String, args: Array[String] = Array[String](), description: String = "")