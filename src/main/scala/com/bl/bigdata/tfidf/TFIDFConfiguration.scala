package com.bl.bigdata.tfidf


import com.bl.bigdata.util.ConfigurationBL

import scala.xml.XML

/**
  * Created by MK33 on 2016/3/10.
  */
object TFIDFConfiguration extends ConfigurationBL{
  def loadResource(path: String): Unit = {

    val xml = XML.load(path)

    val properties = xml \ "property"

  }
}
