package com.bl.bigdata.personal

import scala.collection.JavaConversions._
/**
 * Created by MK33 on 2016/6/13.
 */
object Test {

  def main(args: Array[String]) {

    val p = System.getenv().keySet()
    val a =System.getProperties.stringPropertyNames().iterator()
    while (a.hasNext){
      val t = a.next()
      if (t.startsWith("s"))
      println(t +  "   ====  " + System.getProperty(t))
    }
    for (pp <- p){
      println(pp + "   ========   " + System.getenv(pp))
    }

  }
}
