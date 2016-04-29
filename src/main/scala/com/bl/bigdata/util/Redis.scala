package com.bl.bigdata.util

/**
 * Created by MK33 on 2016/4/27.
 */
trait Redis {
  ConfigurationBL.addResource("redis.xml")
  val host: String = ConfigurationBL.get("redis.host")
  val port: Int = ConfigurationBL.get("redis.port").toInt
  val timeout: Int = ConfigurationBL.get("redis.timeout").toInt
}
