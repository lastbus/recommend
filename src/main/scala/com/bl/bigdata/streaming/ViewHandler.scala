package com.bl.bigdata.streaming

import org.apache.hadoop.hbase.client.{Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager
import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
 * Created by MK33 on 2016/6/16.
 */

class ViewHandler(table: Table, myJedis: Jedis) extends Handler  {
  val logger = LogManager.getLogger(this.getClass.getName)

  override val hTable: Table = table
  override val jedis: Jedis = myJedis

  val columnFamilyBytes = Bytes.toBytes("member_id")

  override def handle(json: JSONObject): Unit = {

    val cookieID = json.getString("cookieId")
    val get = new Get(Bytes.toBytes(cookieID))
    if (hTable.exists(get)) {
      val memberIDBytes = hTable.get(get).getValue(columnFamilyBytes, columnFamilyBytes)
      val memberId = new String(memberIDBytes)
      logger.debug(json.getString("eventDate"))
      val map = Map(json.getString("eventDate") -> json.getString("goodsId"))
      jedis.hmset("rcmd_rt_view_" + memberId, map)
      jedis.expire("rcmd_rt_view_" + memberId, 3600 * 10)
      logger.debug(s"redis key: rcmd_rt_view_$memberId, value: $map")
    } else {
      logger.info("cookieID \"" + cookieID + "\" not found in hbase")
    }

  }
}
