package com.bl.bigdata.goods

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/5/16.
 */
class GoodsOffShelf {
  def offShelf(): Unit = {
    val sc = SparkFactory.getSparkContext("offShelf")
    val hiveContext = new HiveContext(sc)
    val sql = "select goods_sid,channel_sid from recommendation.goods_off_shelf"
    val gRdd = hiveContext.sql(sql).rdd.map(row =>(row.getString(0),row.getString(1).toInt))
    gRdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        //3 PC
        if(s._2 == 3) {
          jedis.del("rcmd_orig_pc_" + s._1)
        }
          //1app
        else if(s._2 ==1)
        {
            jedis.del("rcmd_orig_app_" + s._1)
        }

      })
      jedis.close()
    })
    sc.stop();
  }
}

object GoodsOffShelf {
  def main(args: Array[String]) {
    val gp  = new GoodsOffShelf
    gp.offShelf()
  }
}
