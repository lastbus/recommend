package com.bl.bigdata.util


/**
  * Created by MK33 on 2016/3/10.
  */
abstract class ConfigurableBL {

  def addResource(path: String)
  def getOption(key: String): Option[String]
  def getAll: Iterable[(String, String)]
  def get(key: String): String = getOption(key).getOrElse(throw new NoSuchElementException)
  def get(key: String, defaultValue: String): String = getOption(key).getOrElse(defaultValue)

}
