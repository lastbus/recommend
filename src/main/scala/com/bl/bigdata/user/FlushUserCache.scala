package com.bl.bigdata.user

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/6/21.
 */
class FlushUserCache {

  def flushglCache():Unit={

    val sc = SparkFactory.getSparkContext("flush_user_cache")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select member_id from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=1000"
    val memRdd  = hiveContext.sql(sql).rdd.map(row=>row.getString(0)).distinct()
    memRdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        if(jedis.exists("pcgl_"+ s)) {
          println("pcgl_"+ s)
          jedis.del("pcgl_"+ s)
        }
        if(jedis.exists("appgl_"+s))
          {
            println("appgl_"+s)
            jedis.del(("appgl_"+s))
          }
      })
      jedis.close()
    })
    sc.stop()
  }
}

object FlushUserCache {
  def main(args: Array[String]): Unit = {
    val fuc = new FlushUserCache()
    fuc.flushglCache
  }

}
