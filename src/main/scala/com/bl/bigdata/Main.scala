package com.bl.bigdata

import com.bl.bigdata.mail.{Message, MailServer}
import com.bl.bigdata.ranking.{GoodsForSale, HotSaleGoods}
import com.bl.bigdata.similarity.{BuyGoodsSimilarity, SeeBuyGoodsSimilarity, BrowserGoodsSimilarity}
import com.bl.bigdata.tfidf.GoodsSimilarityInCate
import com.bl.bigdata.useranalyze.{BrowserNotBuy, BuyActivityStatistic, CategorySimilarity}
import com.bl.bigdata.util.{ToolRunner, RedisClient, Tool}
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by MK33 on 2016/3/30.
  */
object Main {

  def main(args: Array[String]) {
    val main = new Main with ToolRunner
    main.run(args)


//    val sparkConf = new SparkConf().setMaster("local").setAppName("tt")
//    val sc = new SparkContext(sparkConf)
//    val d = sc.parallelize(Seq(("a", "b")))
//    println("=======11111111111===========")
//    d.foreachPartition( p => {
//      println("=====222222=======")
//      val jedis = RedisClient.pool.getResource
//    jedis.flushAll()
//      p.foreach(t => {println(t); jedis.set(t._1, t._2)})
//      jedis.close()
//      println("-----------------")
//    })
//    println("=======5555555555555=====")
//
//    sc.stop()

  }



}

class Main extends Tool{

  override def run(args: Array[String]): Unit = {

//    try {
//      // 看了又看
//      BrowserGoodsSimilarity.execute(args)
//    } catch {
//      case e: Exception =>
//        Message.message.append(s"看了又看出错： ${e.getMessage}\n\n")
//        Thread.sleep(10 * 1000)
//    }
//
//    try {
//      // 看了最终买
//      SeeBuyGoodsSimilarity.execute(args)
//    } catch {
//      case e: Exception => Message.message.append(s"看了最终买出错： ${e.getMessage}\n\n")
//        Thread.sleep(15 * 1000)
//    }
//    try {
//      // 买了还买
//      BuyGoodsSimilarity.execute(args)
//    } catch {
//      case e: Exception =>
//        Message.message.append(s"买了还买出错： ${e.getMessage}\n\n")
//        Thread.sleep(15 * 1000)
//    }
//    try {
//      // 品类买了还买
//      CategorySimilarity.execute(args)
//    } catch {
//      case e: Exception =>
//        Message.message.append(s"品类买了还买出错： ${e.getMessage}\n\n")
//        Thread.sleep(15 * 1000)
//    }
//    try {
//      // 上午 下午 晚上 购买类目
//      BuyActivityStatistic.execute(args)
//    } catch {
//      case e: Exception =>
//        Message.message.append(s"上午下午晚上购买类目出错： ${e.getMessage}\n\n")
//        Thread.sleep(10 * 1000)
//    }
//    try {
//      // 最近两个月浏览未购买商品 按时间排序
//      BrowserNotBuy.execute(args)
//    } catch {
//      case e: Exception =>
//        Message.message.append(s"最近一段时间浏览未购买的商品： ${e.getMessage}\n\n")
//        Thread.sleep(10 * 1000)
//    }
    try {
      // 品类热销商品
      HotSaleGoods.execute(args)
    } catch {
      case e: Exception =>
        Message.message.append(s"热销商品出错： ${e.getMessage}\n\n")
        Thread.sleep(10 * 1000)
    }

//    try {
//      GoodsForSale.execute(args)
//    } catch {
//      case e: Exception =>
//        Message.message.append(s"error in goods available for sale : ${e.getMessage}\n\n" )
//        Thread.sleep(10 * 1000)
//    }
//    try {
//      GoodsSimilarityInCate.execute(args)
//    } catch {
//      case e: Exception =>
//        Message.message.append(s"同类商品相似性出错 ${e.getMessage}\n\n")
//        Thread.sleep(10 * 1000)
//    }

//    MailServer.send(Message.message.toString())

  }
}
