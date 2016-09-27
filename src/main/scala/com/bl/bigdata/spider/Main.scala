package com.bl.bigdata.spider

import org.jsoup.Jsoup

/**
  * Created by MK33 on 2016/8/16.
  */
object Main {

  def main(args: Array[String]): Unit = {
    val html = Jsoup.connect("http://ent.ifeng.com/").get()
    val keyWords = Array("王宝强","宝强", "马蓉", "宋喆")
//    println(html)
//    println(html.select("div.col_left .box_hots"))
    val header = html.select("div.col_left .box_hots")
    Console println header.size()
    val iterator = header.iterator()
    while (iterator.hasNext) {
      val block = iterator.next()
      val text = block.text()
      val has = for (item <- keyWords if text.contains(item)) yield true
      if (has.contains(true)) {
        val hrefs = block.select("a")
        val size = hrefs.size()
        for (i <- 0 until size) {
          val ele = hrefs.get(i)
          val url = ele.attr("href").trim
          val title = ele.text()
          println(url + " " + title)
          val sql = "insert into news (url, title) values(" + "  \"" + url + "\"  , \"" + title + "\") "
          println(sql)
          MysqlConnection.st.execute(sql)
        }
      }
    }


  }


}
