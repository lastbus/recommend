package com.bl.bigdata.streaming

import org.apache.hadoop.hbase.client.Table
import org.json.JSONObject
import redis.clients.jedis.Jedis

/**
 * Created by MK33 on 2016/6/16.
 */
trait Handler {

  val hTable: Table
  val jedis: Jedis
  def handle(jSONObject: JSONObject)

}
