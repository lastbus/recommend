package com.bl.bigdata.streaming

import com.bl.bigdata.util.RedisClient
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Get, Put, Table}
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
  lazy val viewTable = HBaseConnectionPool.connection.getTable(TableName.valueOf("rcmd_rt_tbl"))

  lazy val jedis = RedisClient.jedisCluster
  val columnFamilyBytes = Bytes.toBytes("member_id")
  val redisPrefix = "goods_view_count_"

  /**
    * channel：
    *   1) ：PC 传cookieID
    *   2) : APP 暂时没有
    *   3) : H5 传 member_id
    * @param json
    */
  def handle(json: JSONObject): Unit = {
    val goodsID = json.getString("goodsId")
    val eventDate = json.getString("eventDate")
    val channel = json.getString("channel").toLowerCase

    // 统计商品浏览次数
    if (jedis.exists(redisPrefix + goodsID))
      jedis.incr(redisPrefix + goodsID)
    else
      jedis.set(redisPrefix + goodsID, "1")
    channel match {
      case "pc" =>
        val cookieID = json.getString("cookieId")
        if (cookieID == null | cookieID.length == 0) {
          logger.error(s"cannot found cookie id : ${json.toString}")
          return
        }
        val get = new Get(Bytes.toBytes(cookieID))
        if (hTable.exists(get)) {
          val memberIDBytes = hTable.get(get).getValue(columnFamilyBytes, columnFamilyBytes)
          val memberId = new String(memberIDBytes)
          logger.debug(json.getString("eventDate"))
          val map = Map(eventDate -> goodsID)
          jedis.hmset("rcmd_rt_view_" + memberId, map)
          jedis.expire("rcmd_rt_view_" + memberId, 3600 * 10)
          logger.debug(s"redis key: rcmd_rt_view_$memberId, value: $map ")
          viewTable.put(new Put(Bytes.toBytes(memberId)).addColumn(Bytes.toBytes("view"), Bytes.toBytes(eventDate), Bytes.toBytes(goodsID)))
        } else {
          logger.info("cookieID \"" + cookieID + "\" not found in hbase")
        }
      case "app" => logger.info("## app ##")
      case "h5" =>
        val memberId = json.getString("memberId")
        val map = Map(eventDate -> goodsID)
        jedis.hmset("rcmd_rt_view_" + memberId, map)
        jedis.expire("rcmd_rt_view_" + memberId, 3600 * 10)
        viewTable.put(new Put(Bytes.toBytes(memberId)).addColumn(Bytes.toBytes("view"), Bytes.toBytes(eventDate), Bytes.toBytes(goodsID)))

    }



  }
}
