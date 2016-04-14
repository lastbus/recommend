package com.bl.bigdata


import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.ranking.{GoodsForSale, HotSaleGoods}
import com.bl.bigdata.similarity.{BrowserGoodsSimilarity, BuyGoodsSimilarity, SeeBuyGoodsSimilarity}
import com.bl.bigdata.tfidf.GoodsSimilarityInCate
import com.bl.bigdata.useranalyze.{BrowserNotBuy, BuyActivityStatistic, CategorySimilarity, UserCookie}
import com.bl.bigdata.util._

import scala.collection.mutable.ListBuffer


/**
  * Created by MK33 on 2016/3/30.
  */
object Main {

  def main(args: Array[String]) {
    val main = new Main with ToolRunner
    main.run(args)
  }

}

class Main extends Tool {

  override def run(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val date = new Date()
    Message.addMessage(s"============  task begin at : ${sdf.format(date)} ========= \n\n\n")
    val list = new ListBuffer[Tool]
    if (args.length == 0) {
      list += new BrowserGoodsSimilarity with ToolRunner // 看了又看
      list += new SeeBuyGoodsSimilarity with ToolRunner // 看了最终买
      list += new BuyGoodsSimilarity with ToolRunner // 买了还买
      list += new CategorySimilarity with ToolRunner // 品类买了还买
      list += new BuyActivityStatistic with ToolRunner // 上午 下午 晚上 购买类目
      list += new BrowserNotBuy with ToolRunner // 最近两个月浏览未购买商品 按时间排序
      list += new HotSaleGoods with ToolRunner // 品类热销商品
      list += new GoodsForSale with ToolRunner // goods for sale
      list += new GoodsSimilarityInCate with ToolRunner // 同一类别商品的相似度
      list += new UserCookie with ToolRunner // 将用户 ID 和 cookieID 导入到 redis
    } else {
      val map: Map[String, Tool] = Map("BrowserGoodsSimilarity".toLowerCase -> new BrowserGoodsSimilarity with ToolRunner,
                            "SeeBuyGoodsSimilarity".toLowerCase -> new SeeBuyGoodsSimilarity with ToolRunner,
                            "BuyGoodsSimilarity".toLowerCase -> new BuyGoodsSimilarity with ToolRunner,
                            "CategorySimilarity".toLowerCase -> new CategorySimilarity with ToolRunner,
                            "BuyActivityStatistic".toLowerCase -> new BuyActivityStatistic with ToolRunner,
                            "BrowserNotBuy".toLowerCase -> new BrowserNotBuy with ToolRunner,
                            "HotSaleGoods".toLowerCase -> new HotSaleGoods with ToolRunner,
                            "GoodsForSale".toLowerCase -> new GoodsForSale with ToolRunner,
                            "GoodsSimilarityInCate".toLowerCase -> new GoodsSimilarityInCate with ToolRunner,
                            "UserCookie".toLowerCase -> new UserCookie with ToolRunner)
      for (arg <- args; key = arg.toLowerCase if map.contains(key)) list += map(key)
    }
    Message.addMessage(s"there are ${list.size} tasks. \n\n")

    for (tool <- list) {
      try {
        tool.run(args)
      } catch {
        case e: Exception =>
          Message.addMessage(s"运行出错： ${tool.getClass.getName} \n${e.getMessage}\n\n\n")
          Thread.sleep(5 * 1000)
      }
    }

    val date2 = new Date()
    val t = (date2.getTime - date.getTime) / 1000
    Message.addMessage(s"=============  time taken： $t s =================")
    Message.sendMail
    SparkFactory.destroyResource()

  }
}
