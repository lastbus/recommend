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
  * 计算用户浏览的某类商品之间的相似度
  * 计算方法：
  * 根据用户在某一天内浏览商品的记录去计算相似度。
  * r(A,B) = N(A,B) / N(B)
  * r(B,A) = N(A,B) / N(A)
  * Created by MK33 on 2016/3/14.
  */
class BrowserGoodsSimilarity {

  val isEmpty = (temp: String) => {
    if (temp.trim.length == 0) true
    else if (temp.equalsIgnoreCase("NULL")) true
    else false
  }

   def run(args: Array[String]): Unit = {
    val sc = SparkFactory.getSparkContext("看了又看")
    // 根据最近多少天的浏览记录，默认 90天
    val limit = 90
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
    val sql = "select cookie_id, category_sid, event_date, behavior_type, goods_sid  " +
      "from recommendation.user_behavior_raw_data  where dt >= " + start
    val rawRdd = ReadData.readHive(sc, sql).map{ case Array(cookie, category, date, behaviorId, goodsId) =>
                                              (cookie, category, date.substring(0, date.indexOf(" ")), behaviorId, goodsId) }
                                            .filter(_._4 == "1000")
                                            .map { case (cookie, category, date, behaviorId, goodsId) => ((cookie, category, date), goodsId)}
                                            .filter(v => {
                                              if (v._1._2.trim.length == 0) false
                                              else if (v._1._2.equalsIgnoreCase("NULL")) false
                                              else true })
                                            .distinct()
    rawRdd.cache()
    // 将用户看过的商品两两结合在一起
    val tuple = rawRdd.join(rawRdd).filter { case (k, (v1, v2)) => v1 != v2 }
                                    .map { case (k, (goodId1, goodId2)) => (goodId1, goodId2) }
    // 计算浏览商品 (A,B) 的次数
    val tupleFreq = tuple.map((_, 1)).reduceByKey(_ + _)
    // 计算浏览每种商品的次数
    val good2Freq = rawRdd.map(_._2).map((_, 1)).reduceByKey(_ + _)
    val good1Good2Similarity = tupleFreq.map { case ((good1, good2), freq) => (good2, (good1, freq)) }
                                        .join(good2Freq)
                                        .map { case (good2, ((good1, freq), good2Freq1)) => (good1, Seq((good2, freq.toDouble / good2Freq1))) }
                                        .reduceByKey((s1, s2) => s1 ++ s2)
                                        .mapValues(v => v.sortWith(_._2 > _._2).take(20))
                                        .map { case (goods1, goods2) => { (goods1, goods2.map(_._1).mkString("#")) }}

     good1Good2Similarity.foreachPartition{
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
           p.addColumn(HbaseConfig.similar_goods_familay.getBytes, HbaseConfig.similar_goods_view_based_in_category.getBytes,
             Bytes.toBytes(y._2.toString))
           cookieTbl.put(p)
         }
         }
         cookieTbl.flushCommits()
       }
     }

  }

}

object BrowserGoodsSimilarity {

  def main(args: Array[String]) {
        execute(args)
  }

  def execute(args: Array[String]) = {
    val goodsSimilarity = new BrowserGoodsSimilarity
    goodsSimilarity.run(args)
  }
}
