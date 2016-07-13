package com.bl.bigdata.streaming

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager
import org.json.JSONObject

/**
  * Created by MK33 on 2016/7/8.
  */
object GYLHandler {
  val logger = LogManager.getLogger(this.getClass.getName)

  val hTable = HBaseConnectionPool.connection.getTable(TableName.valueOf("recommend"))
  val cal = Calendar.getInstance()
  val familyBytes = Bytes.toBytes("recommend")
  val columnBytes = Bytes.toBytes("gyl")
  val sdf = new SimpleDateFormat("yyyyMMdd")

  def handler(json: JSONObject): Unit = {
    val memberID = json.getString("memberId")
    val channel = json.getString("channel")
    val eventDate = json.getString("eventDate").toLong
    val goodsList = json.getJSONObject("recResult").getJSONArray("goodsList")

    val date = sdf.format(new Date(eventDate))

    val j = new JSONObject()
    j.put("goodsList", goodsList)
    val put = new Put(Bytes.toBytes(memberID.reverse + "-" + channel + "-" + date ))
    put.addColumn(familyBytes, columnBytes, Bytes.toBytes(j.toString()))
    hTable.put(put)

  }

  def main(args: Array[String]) {
    val c = Calendar.getInstance()
    c.setTime(new Date(1467947303691L))
    println(c.get(Calendar.YEAR))
    println(c.get(Calendar.MONTH))
    println(c.get(Calendar.DAY_OF_MONTH))

  }

}
