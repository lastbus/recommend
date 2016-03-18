package com.bl.bigdata.similarity

import scala.collection.mutable
import scala.xml.XML

/**
  * 读取配置文件
  * Created by MK33 on 2016/3/14.
  */
object ItemSimilarityConf {

  var confMap: mutable.HashMap[String, String] = _


  def loadConf(path: String): Unit = {
    val xml =
      try{
        XML.load(path)
      } catch {
        case _: Exception => throw new Exception(s"error in parse configuration file :  $path.")
      }

    val tmpMap = mutable.HashMap.empty[String, String]
    val properties = xml \ "property"
    for (property <- properties){
      val name = (property \ "name").text
      val value = (property \ "value").text
      tmpMap(name) = value
    }
    confMap = tmpMap
  }

}
