package com.bl.bigdata.util

import java.util

/**
 * Created by blemall on 3/23/16.
 */
object Implicts {

  implicit def map2HashMap(map: Map[String, String]): util.HashMap[String, String] = {
    val m = new util.HashMap[String, String]
    for ((k, v) <- map) m.put(k, v)
    m
  }
}
