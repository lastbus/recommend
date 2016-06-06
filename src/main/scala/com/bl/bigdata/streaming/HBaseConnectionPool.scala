package com.bl.bigdata.streaming

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
 * Created by MK33 on 2016/5/24.
 */
object HBaseConnectionPool {

  lazy val connection = {
    val conf = HBaseConfiguration.create()
    ConnectionFactory.createConnection(conf)
  }

}
