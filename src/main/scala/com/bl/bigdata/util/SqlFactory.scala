package com.bl.bigdata.util


import org.apache.logging.log4j.LogManager

import scala.collection.mutable
import scala.xml.XML

/**
  * Created by MK33 on 2016/10/26.
  */
private[bigdata] object SqlFactory {

  private val logger = LogManager.getLogger(this.getClass.getName)

  lazy private val sqlMap = {
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("hive.xml")
    if (inputStream == null) {
      logger.error(s"cannot found hive.xml ")
      sys.exit(-1)
    }
    val xml = XML.load(inputStream)
    val root = xml \ "sql"
    val sqls = mutable.Map[String, String]()
    for (sql <- root) {
      val name = (sql \ "name").text
      val value = (sql \ "value").text
      logger.debug("\n" + name + "\n" + value)
      sqls(name) = value
    }
    sqls.toMap
  }

  /**
    * 根据 sql 的名字获取配置文件中的 sql
    * @param sqlName  配置文件中的 sql 名称
    * @param parameters sql 中的参数
    * @return sql
    */
  def getSql(sqlName: String, parameters: String*): String =  {
    val sql = sqlMap(sqlName)
    if (parameters.isEmpty) sql else String.format(sql, parameters: _*)
  }


}
