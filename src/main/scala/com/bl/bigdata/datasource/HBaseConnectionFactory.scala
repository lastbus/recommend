package com.bl.bigdata.datasource

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}

/**
  * Created by MK33 on 2016/11/2.
  */
object HBaseConnectionFactory {

  private var connection: Connection = _
  private var table: Table = _

  def getConnection(tableName: String): Table = {
    if (table == null) {
      val conn = getConnection
      table = conn.getTable(TableName.valueOf(tableName))
    }
    table
  }

  def getConnection:  Connection = {
    if (connection == null || connection.isClosed) {
      connection = ConnectionFactory.createConnection()
    }
    connection
  }

  sys.addShutdownHook(new Thread {
    override def run(): Unit = {
      println("begin to shutdown hbase connection")
      if (connection != null) {
        connection.close()
      }
      println("finished shutdown hbase connection")
    }
  }.start)



}
