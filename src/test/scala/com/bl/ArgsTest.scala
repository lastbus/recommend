package com.bl

import java.io.IOException
import java.util
import java.util.Properties

import org.apache.hadoop.util.ProgramDriver

import scala.collection.JavaConversions._

/**
 * Created by MK33 on 2016/6/9.
 */
object ArgsTest {

  def main(args: Array[String]) {

    val start: Long = System.currentTimeMillis

    val mainClass = loadProperties("recommend.class.props")
    if (mainClass == null) throw new IOException("cannot load any recommend.props")

    var foundShortName = false
    val programDriver = new ProgramDriver()
    for (className <- mainClass.stringPropertyNames())
    {
      println(className)
      println(args(0))
      if (args.length > 0 && getShortName(mainClass.getProperty(className)).equals(args(0)) )
        foundShortName = true
      if (args.length > 0 && className.equalsIgnoreCase(args(0)) && isDeprecated(mainClass, className)) {
        println(desc(mainClass.getProperty(className)))
        return
      }

      if (!isDeprecated(mainClass, className)) {
        println(mainClass.getProperty(className))
        addClass(programDriver, className, mainClass.getProperty(className))
      }
    }

    if (args.length < 1 || args(0) == null || ("-h" == args(0)) || ("--help" == args(0))) {
      programDriver.driver(args)
      return
    }

    val progName: String = args(0)
    if (!foundShortName) {
      addClass(programDriver, progName, progName)
    }
    shift(args)

    println(args(0))

    var mainProps: Properties = loadProperties(progName + ".props")
    if (mainProps == null) {
      println(s"No $progName.props found on classpath, will use command-line arguments only")
      mainProps = new Properties
    }

    var i = 0
    val argMap: util.Map[String, Array[String]] = new util.HashMap[String, Array[String]]
    while (i < args.length && args(i) != null) {
      val argValues: util.List[String] = new util.ArrayList[String]
      var arg: String = args(i)
      println("=========  " + i +"    :    " +  args(i) + "  =========")
      i += 1
      if (arg.startsWith("-D")) {
        val argSplit: Array[String] = arg.split("=")
        arg = argSplit(0)
        if (argSplit.length == 2) {
          argValues.add(argSplit(1))
        }
      }
      else {
        while (i < args.length && args(i) != null && !args(i).startsWith("-")) {
          println("argValues.add(args(i))      ::     " + args(i))
          argValues.add(args(i))
          i += 1
        }
      }
      println(argValues.mkString(","))
      argMap.put(arg, argValues.toArray(new Array[String](argValues.size)))
    }

    println("\n\n===============   argsMap   =============")
    argMap.foreach(s => println(s._1 + " :: " + s._2.mkString("#")))

    // Add properties from the .props file that are not overridden on the command line
    for (key <- mainProps.stringPropertyNames) {
      println(key)
      val argNamePair: Array[String] = key.split("\\|")
      println(argNamePair.mkString(", "))
      val shortArg: String = '-' + argNamePair(0).trim
      val longArg: String = if (argNamePair.length < 2) null else "--" + argNamePair(1).trim
      println(shortArg)
      println(longArg)
      if (!argMap.containsKey(shortArg) && (longArg == null || !argMap.containsKey(longArg))) {
        argMap.put(longArg, Array[String](mainProps.getProperty(key)))
      }
    }

    println("===========   add command-line args   ==========")
    // Now add command-line args
    val argsList: util.List[String] = new util.ArrayList[String]
    argsList.add(progName)
    for (entry <- argMap.entrySet) {
      println(entry.getKey + "   " + entry.getValue.mkString(","))
      var arg: String = entry.getKey
      if (arg.startsWith("-D")) {
        val argValues: Array[String] = entry.getValue
        if (argValues.length > 0 && !argValues(0).trim.isEmpty) {
          arg += '=' + argValues(0).trim
        }
        println(arg)
        argsList.add(1, arg)
      } else {
        argsList.add(arg)
        for (argValue <- argMap.get(arg)) {
          if (!argValue.isEmpty) {
            argsList.add(argValue)
            println(argValue)
          }
        }
      }
    }
    println(argsList.mkString("#"))
//    programDriver.driver(argsList.toArray(new Array[String](argsList.size)))

    println(s"Program took ${System.currentTimeMillis - start} ms (Minutes: ${(System.currentTimeMillis - start) / 60000.0})")


  }

  def shift(args: Array[String]): Array[String] = {
    System.arraycopy(args, 1, args, 0, args.length - 1)
    args(args.length - 1) = null
    args
  }

  private def  desc (valueString : String): String = {
    if (valueString.contains(":")) valueString.substring(valueString.indexOf(':')).trim else valueString
  }


  private def isDeprecated(mainClasses: Properties, keyString: String): Boolean = {
    "deprecated".equalsIgnoreCase(getShortName(mainClasses.getProperty(keyString)))
  }

  def getShortName(s: String): String = {
    if (s.contains(":")) s.substring(0, s.indexOf(':')).trim else s.trim
  }

  def getDesc(s: String): String = {
    if (s.contains(":")) s.substring(s.indexOf(':')) else s
  }
  def loadProperties(fileName: String): Properties = {
    val in = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName)
    if (in != null) {
      try {
        val props = new Properties
        props.load(in)
        props
      } finally {
        in.close()
      }
    } else null
  }

  def addClass(programDriver: ProgramDriver, className: String, desc: String): Unit = {
    try {
      val clazz = Class.forName(className)
      programDriver.addClass(getShortName(desc), clazz, getDesc(desc))
    } catch {
      case e: Throwable => println("unable to add class {} " + className + " : " + e)
    }


  }



}
