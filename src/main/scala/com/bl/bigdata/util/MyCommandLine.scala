package com.bl.bigdata.util

import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}

import scala.collection.mutable

/**
  * Created by MK33 on 2016/7/13.
  */
class MyCommandLine(name: String) {
  private val optionMap = mutable.Map[String, String]()
  val options = new Options
  options.addOption("h", "help", false, "print help information")
  /** 命令说明 */
  def addOption(shortName: String, longName: String, hasArgs: Boolean, desc: String): Unit ={
    options.addOption(shortName, longName, hasArgs, desc)
  }
  /** 是否必须 */
  def addOption(shortName: String, longName: String, hasArgs: Boolean, desc: String, mustHaveValue: Boolean): Unit = {
    if (mustHaveValue) {
      val option = new Option(shortName, longName, hasArgs, desc)
      option.setRequired(true)
      options.addOption(option)
    } else addOption(shortName, longName,hasArgs, desc)
  }
  /** 默认值 */
  def addOption(shortName: String, longName: String, hasArgs: Boolean, desc: String, defaultValue: String): Unit ={
    addOption(shortName, longName,hasArgs, desc)
    optionMap(longName) = defaultValue
  }
  def addOption(option: Option): Unit = {
    addOption(option)
  }

  val parser = new BasicParser

  def parser(args: Array[String]): Map[String, String] = {
    val commandLineParser = parser.parse(options, args)

    if (commandLineParser.hasOption("h")){
      printHelper
      sys.exit(0)
    }

    for (op <- commandLineParser.getOptions){
      if (op.hasArg)
        optionMap(op.getLongOpt) = op.getValue
      else
        optionMap(op.getLongOpt) = "true"
    }
    optionMap.toMap
  }

  def printHelper = {
    val helper = new HelpFormatter
    helper.printHelp(name, options)
  }

}

object MyCommandLine {

  def main(args: Array[String]) {
    val cmd = new MyCommandLine("test")
    cmd.addOption("a", "abc", true, "d")
    cmd.addOption("b", "bcd", false, "dd")
    cmd.parser(args)
  }

}