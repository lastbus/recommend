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

  lazy val hTable: Table = HBaseConnectionPool.connection.getTable(TableName.valueOf("recommend"))
  val cal = Calendar.getInstance()
  val columnFamilyBytes = Bytes.toBytes("recommend")
  val columnBytes = Bytes.toBytes("pcgl")
  val sdf = new SimpleDateFormat("yyyyMMdd")

  def handler(json: JSONObject): Unit = {
    val eventDate = json.getString("eventDate").toLong
    val memberId = json.getString("memberId")
    val channel = json.getString("channel")
    val recResult = json.getJSONObject("recResult").getJSONArray("goodsList")

    val date = sdf.format(new Date(eventDate))
    val j = new JSONObject()
    j.put("goodsList", recResult)

    val put = new Put(Bytes.toBytes(memberId.reverse + "-" + channel + "-" + date))
    put.addColumn(columnFamilyBytes, columnBytes, Bytes.toBytes(j.toString()))

    hTable.put(put)

  }



}
