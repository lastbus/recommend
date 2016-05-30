package com.bl.bigdata.HBase

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by MK33 on 2016/5/27.
 */
object Test {

  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create()

    val table = new HTable(conf, Bytes.toBytes(""))

    val hbaseAdmin = new HBaseAdmin(conf)

    val conn = ConnectionFactory.createConnection(conf)
    val table2: Table = conn.getTable(TableName.valueOf(""))
    val admin: Admin = conn.getAdmin

    val htablePool = new HTablePool(conf, 10)
    val tt = htablePool.getTable("")



  }

}
