package com.bl.bigdata.goods

import java.util
import java.util.Date

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.sql.hive.HiveContext

;

/**
 * Created by HJT20 on 2016/5/20.
 */
class GoodsAvailableForSale {

  def gatoRedis(): Unit = {

  val sql = "select sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, CASE  WHEN brand_sid IS NULL THEN 'null' else brand_sid END AS brand_sid, CASE  WHEN cn_name IS NULL THEN 'xxx' else cn_name END AS cn_name, category_id, category_name, sale_price, pic_sid, url, channel_sid  " +
    ",stock,yun_type,store_sid,com_sid,sale_status from recommendation.goods_avaialbe_for_sale_channel where category_id IS NOT NULL and yun_type is not null"
  val sc = SparkFactory.getSparkContext("goods_availabl_for_sale")
  val hiveContext = new HiveContext(sc)
    val r = hiveContext.sql(sql).rdd
    .map(row => (row.getString(0), row.getString(1), row.getString(2),
      row.getDouble(3).toString, row.getString(4), row.getString(5),
      row.getString(6), row.getLong(7).toString, row.getString(8),
      row.getDouble(9).toString, row.getString(10), row.getString(11),
      row.getString(12),row.getInt(13).toString,row.getDouble(14).toString,row.getString(15),row.getString(16),row.getInt(17).toString))
    .distinct()
    .map { case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
    category_id, category_name, sale_price, pic_id, url, channel,stock,yun_type,store_sid,com_sid,sale_status) =>
      val updateTime =  (new Date()).getTime.toString
      val map = Map("sid" -> sid, "mdm_goods_sid" -> mdm_goods_sid,
        "goods_sales_name" -> goods_sales_name, "goods_type" -> goods_type,
        "pro_sid" -> pro_sid, "brand_sid" -> brand_sid,
        "cn_name" -> cn_name, "category_id" -> category_id,
        "category_name" -> category_name, "sale_price" -> sale_price,
        "pic_sid" -> pic_id, "url" -> url,"stock"->stock.toString,"yun_type"->yun_type.toString,"store_sid"->store_sid,"com_sid"->com_sid,"sale_status"->sale_status,"update"->updateTime)
      val m = new util.HashMap[String, String]
      for ((k, v) <- map) m.put(k, v)
      val terminal = if (channel.equals("3")) "pc_" else if(channel.equals("1")) "app_" else "h5_"
      ("rcmd_orig_" + terminal + sid, m)
    }

    r.foreachPartition(partition => {
      try {
        val jedisCluster = RedisClient.jedisCluster
        partition.foreach(data => {
          println(data._1)
          jedisCluster.hmset(data._1, data._2)
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }

    })



    sc.stop()
  }
}

object GoodsAvailableForSale {
  def main(args: Array[String]) {
    val gp  = new GoodsAvailableForSale
    gp.gatoRedis()
  }
}