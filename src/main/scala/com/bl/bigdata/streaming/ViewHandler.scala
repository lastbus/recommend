package com.bl.bigdata.streaming

import com.bl.bigdata.util.RedisClient
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager
import org.json.JSONObject

import scala.collection.JavaConversions._

/**
 * Created by MK33 on 2016/6/16.
 */

object ViewHandler {

  val logger = LogManager.getLogger(this.getClass.getName)

  lazy val hTable: Table = HBaseConnectionPool.connection.getTable(TableName.valueOf("member_cookie_mapping"))
  lazy val jedis = RedisClient.jedisCluster
  val columnFamilyBytes = Bytes.toBytes("member_id")
  val redisPrefix = "goods_view_count_"

  def handle(json: JSONObject): Unit = {
    println("=======================")
    println(logger.getName)
    val cookieID = json.getString("cookieId")
    val goodsID = json.getString("goodsId")
    val get = new Get(Bytes.toBytes(cookieID))
    // 统计商品浏览次数
    if (jedis.exists(redisPrefix + goodsID))
      jedis.incr(redisPrefix + goodsID)
    else
      jedis.set(redisPrefix + goodsID, "1")

    if (hTable.exists(get)) {

      val memberIDBytes = hTable.get(get).getValue(columnFamilyBytes, columnFamilyBytes)
      val memberId = new String(memberIDBytes)
      logger.debug(json.getString("eventDate"))
      val map = Map(json.getString("eventDate") -> goodsID)
      jedis.hmset("rcmd_rt_view_" + memberId, map)
      jedis.expire("rcmd_rt_view_" + memberId, 3600 * 10)
      logger.debug(s"redis key: rcmd_rt_view_$memberId, value: $map ")
    } else {
      println("  else ")
      logger.info("cookieID \"" + cookieID + "\" not found in hbase")
    }

  }
}
