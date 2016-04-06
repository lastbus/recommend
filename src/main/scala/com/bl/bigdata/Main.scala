package com.bl.bigdata

import com.bl.bigdata.mail.MailServer
import com.bl.bigdata.ranking.HotSaleGoods
import com.bl.bigdata.similarity.{BuyGoodsSimilarity, BrowserGoodsSimilarity, SeeBuyGoodsSimilarity}
import com.bl.bigdata.useranalyze.{BrowserNotBuy, BuyActivityStatistic, CategorySimilarity}
import com.bl.bigdata.util.{ToolRunner, Tool}

/**
  * Created by MK33 on 2016/3/30.
  */
object Main {

  def main(args: Array[String]) {
    val main = new Main with ToolRunner
    main.run(args)
  }



}

class Main extends Tool{

  override def run(args: Array[String]): Unit = {

    try {
      // 看了又看
      BrowserGoodsSimilarity.execute(args)
    } catch {
      case e: Exception => MailServer.send(s"看了又看出错： ${e.getMessage}")
    }

    try {
      // 看了最终买
      SeeBuyGoodsSimilarity.execute(args)
    } catch {
      case e: Exception => MailServer.send(s"看了最终买出错： ${e.getMessage}")
    }
    try {
      // 买了还买
      BuyGoodsSimilarity.execute(args)
    } catch {
      case e: Exception => MailServer.send(s"买了还买出错： ${e.getMessage}")
    }
    try {
      // 品类买了还买
      CategorySimilarity.execute(args)
    } catch {
      case e: Exception => MailServer.send(s"品类买了还买出错： ${e.getMessage}")
    }
    try {
      // 上午 下午 晚上 购买类目
      BuyActivityStatistic.execute(args)
    } catch {
      case e: Exception => MailServer.send(s"上午下午晚上购买类目出错： ${e.getMessage}")
    }
    try {
      // 最近两个月浏览未购买商品 按时间排序
      BrowserNotBuy.execute(args)
    } catch {
      case e: Exception => MailServer.send(s"最近一段时间浏览未购买的商品： ${e.getMessage}")
    }
    try {
      // 品类热销商品
      HotSaleGoods.execute(args)
    } catch {
      case e: Exception => MailServer.send(s"热销商品出错： ${e.getMessage}")
    }

  }
}
