package com.bl.bigdata.spider

import java.sql.DriverManager

import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/8/16.
  */
object MysqlConnection {
//  val logger = LogManager.getLogger(this.getClass.getName)

  val driver = "com.mysql.jdbc.Driver"
  Class.forName(driver)
  val url = "jdbc:mysql://10.201.129.74:3306/recommend_system"
  val user = "root"
  val password = "bl.com"
  try {
    Class.forName(driver)
  } catch {
    case e: Throwable =>
//      logger.error(e)
      sys.exit(-1)
  }


  lazy val st = {
    val connection = DriverManager.getConnection(url, user, password)
    connection.createStatement()
  }

  lazy val connection = {
    DriverManager.getConnection(url, user, password)
  }
}
