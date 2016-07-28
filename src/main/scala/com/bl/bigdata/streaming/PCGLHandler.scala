package com.bl.bigdata.streaming

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.json.JSONObject

/**
  * Created by MK33 on 2016/7/5.
  */
object PCGLHandler {

  lazy val hTable: Table = HBaseConnectionPool.connection.getTable(TableName.valueOf("rcmd_user_view"))
  val cal = Calendar.getInstance()
  val columnFamilyBytes = Bytes.toBytes("recommend")
  val columnBytes = Bytes.toBytes("pcgl")
  val sdf = new SimpleDateFormat("yyyyMMdd")

  def handler(json: JSONObject): Unit = {
    val memberId = json.getString("memberId")
    if (memberId == null || memberId.length == 0 || memberId.equalsIgnoreCase("NULL")) return
    val eventDate = json.getString("eventDate")
    val channel = json.getString("channel")
    val recResult = json.getJSONObject("recResult").getJSONArray("goodsList")
    val key = channel + "_" + memberId + "_" + eventDate
    val put = new Put(Bytes.toBytes(key))
    put.addColumn(columnFamilyBytes, columnBytes, Bytes.toBytes(recResult.toString()))
    hTable.put(put)
  }



}
