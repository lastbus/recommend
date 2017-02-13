package com.bl.bigdata.streaming

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.LogManager
import org.json.JSONObject

/**
  * Created by MK33 on 2016/7/8.
  */
object GYLHandler {
  val logger = LogManager.getLogger(this.getClass.getName)

  val hTable = HBaseConnectionPool.connection.getTable(TableName.valueOf("rcmd_user_view"))
  val cal = Calendar.getInstance()
  val familyBytes = Bytes.toBytes("recommend")
  val columnBytes = Bytes.toBytes("gyl")
  val sdf = new SimpleDateFormat("yyyyMMdd")

  /**
    * 解析 json 字符串
    * @param json
    */
  def handler(json: JSONObject): Unit = {
    val memberId = json.getString("memberId")
    if (memberId == null || memberId.length == 0 || memberId.equalsIgnoreCase("NULL")) return
    val channel = json.getString("channel")
    val eventDate = json.getString("eventDate")
    val goodsList = json.getJSONObject("recResult").getJSONArray("goodsList")
    val key = channel + "_" + memberId + "_" + eventDate
    val put = new Put(Bytes.toBytes(key))
    put.addColumn(familyBytes, columnBytes, Bytes.toBytes(goodsList.toString()))
    hTable.put(put)
    logger.info(s"hbase key: ${key}")
  }

  def main(args: Array[String]) {
    val c = Calendar.getInstance()
    c.setTime(new Date(1467947303691L))
    println(c.get(Calendar.YEAR))
    println(c.get(Calendar.MONTH))
    println(c.get(Calendar.DAY_OF_MONTH))

  }

}
