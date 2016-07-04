package com.bl.bigdata.goods.useraction

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.config.HbaseConfig
import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util._
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * 计算用户浏览的物品和购买的物品之间的关联度
  * 计算方法：一天之内用户浏览和购买物品的记录。
  * r(seeGoods, buyGoods) = N(seeGoods, buyGoods) / N(buyGoods)
  * Created by MK33 on 2016/3/15.
  */
class BrowsedAndOrderedSimilarity  {

   def run(args: Array[String]): Unit = {
    val sc = SparkFactory.getSparkContext("看了最终买")
    val limit = 90
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
    val sql = "select cookie_id, category_sid, event_date, behavior_type, goods_sid  " +
      "from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

    val rawData = ReadData.readHive(sc, sql).map{ case Array(cookie, category, date, behaviorId, goodsId) =>
                                            ((cookie, category, date.substring(0, date.indexOf(" "))), behaviorId, goodsId)}
    // 用户浏览的商品
    val browserRdd = rawData.filter { case ((cookie, category, date), behavior, goodsID) => behavior.equals("1000") }
                             .map { case ((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID) }.distinct()
    // 用户购买的商品
    val buyRdd = rawData.filter { case ((cookie, category, date), behavior, goodsID) => behavior.equals("4000") }
                         .map { case ((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID) }.distinct()

    // 计算用户浏览和购买的商品之间的相似性：（浏览的商品A， 购买的商品B）的频率除以 B 的频率
    val browserAndBuy = browserRdd.join(buyRdd)
                                  .map{ case ((cookie, category, date), (goodsIDBrowser, goodsIDBuy)) => ((goodsIDBrowser, goodsIDBuy), 1)}
                                  .filter(item => item._1._1 != item._1._2) // 浏览和购买不能是同一个商品
                                  .reduceByKey(_ + _)
                                  .map{ case ((goodsBrowser, goodsBuy), relation) => (goodsBrowser, Seq((goodsBuy, relation)))}
                                  .reduceByKey((s1, s2) =>  s1 ++ s2)
                                  .map(s => {( s._1, s._2.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")) })

    browserAndBuy.foreachPartition{
      x=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
        conf.set("hbase.zookeeper.quorum",HbaseConfig.hbase_zookeeper_quorum )
        conf.set("hbase.master", HbaseConfig.hbase_master)
        val cookieTbl = new HTable(conf, TableName.valueOf(HbaseConfig.goodstbl))
        cookieTbl.setAutoFlush(false, false)
        cookieTbl.setWriteBufferSize(3*1024*1024)
        x.foreach { y => {
          val p = new Put(Bytes.toBytes(y._1.toString))
          p.addColumn(HbaseConfig.similar_goods_familay.getBytes, HbaseConfig.similar_goods_shop_based_in_category.getBytes,
            Bytes.toBytes(y._2.toString))
          cookieTbl.put(p)
        }
        }
        cookieTbl.flushCommits()
      }
    }
  }

}

object BrowsedAndOrderedSimilarity {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val seeBuyGoodsSimilarity = new BrowsedAndOrderedSimilarity
    seeBuyGoodsSimilarity.run(args)
  }
}
