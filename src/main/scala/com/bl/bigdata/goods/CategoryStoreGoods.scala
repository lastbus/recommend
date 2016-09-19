package com.bl.bigdata.goods

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/7/30.
 */
class CategoryStoreGoods {

  def gatoRedis(): Unit = {

    val sql = "select sid,category_id, store_sid from recommendation.goods_avaialbe_for_sale_channel where category_id IS NOT NULL and yun_type is not null and sale_status=4 and stock=1"
    val sc = SparkFactory.getSparkContext("cate_store_goods")
    val hiveContext = new HiveContext(sc)
    val rdd = hiveContext.sql(sql).rdd
      .map(row => (row.getString(0),row.getLong(1).toString, row.getString(2)))
      .distinct()
    val cRdd= rdd.map{case(gId,cId,sId)=>{
      ("rcmd_cate_goods_"+cId,gId)
    }}.mapValues{gId=>Seq(gId)}.reduceByKey(_ ++ _).mapValues(gIds=>gIds.take(200).mkString("#"))


    val csRdd = rdd.filter(!_._3.equals("null")).map{case(gId,cId,sId)=>{
      ("rcmd_cate_goods_" + sId+"_"+cId,gId)
    }}.mapValues{gId=>Seq(gId)}.reduceByKey(_ ++ _).mapValues(gIds=>gIds.take(200).mkString("#"))


    val sRdd = rdd.filter(!_._3.equals("null")).map{case(gId,cId,sId)=>{
      ("rcmd_store_goods_"+ sId,gId)
    }}.mapValues{gId=>Seq(gId)}.reduceByKey(_ ++ _).mapValues(gIds=>gIds.take(200).mkString("#"))



      cRdd.foreachPartition(partition => {
        try {
          val jedisCluster = RedisClient.jedisCluster
          partition.foreach(data => {
            println(data._1)
            jedisCluster.set( data._1, data._2)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        }

      })

      csRdd.foreachPartition(partition => {
        try {
          val jedisCluster = RedisClient.jedisCluster
          partition.foreach(data => {
            println(data._1)
            jedisCluster.set( data._1, data._2)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        }

      })

      sRdd.foreachPartition(partition => {
        try {
          val jedisCluster = RedisClient.jedisCluster
          partition.foreach(data => {
            println(data._1)
            jedisCluster.set( data._1, data._2)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        }

      })


    sc.stop()
  }


}

object CategoryStoreGoods {

  def main(args: Array[String]) {
    val csg = new CategoryStoreGoods
    csg.gatoRedis()
  }
}
