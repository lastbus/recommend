package com.bl.bigdata.datasource

import com.bl.bigdata.streaming.HBaseConnectionPool
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by MK33 on 2016/7/21.
  */
object HBase {

  def main(args: Array[String]) {

    val conf = HBaseConfiguration.create()

    val conn = HBaseConnectionPool.connection.getTable(TableName.valueOf("rcmd_user_view"))

    val scanner = conn.getScanner(Bytes.toBytes("recommend"), Bytes.toBytes("pcgl"))
    println(scanner.next())

  }

}
