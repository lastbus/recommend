package com.bl.bigdata.streaming

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
 * Created by MK33 on 2016/5/24.
 */
object HBaseConnectionPool {

  lazy val connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    ConnectionFactory.createConnection(conf)
  }

}
