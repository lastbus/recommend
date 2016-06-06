package com.bl.bigdata.util

import com.bl.bigdata.mail.Message
import com.bl.bigdata.product.GoodsNewArrival
import com.bl.bigdata.ranking.{GoodsForSale, HotSaleGoods}
import com.bl.bigdata.search.Search
import com.bl.bigdata.similarity.{BuyGoodsSimilarity, SeeBuyGoodsSimilarity, BrowserGoodsSimilarity}
import com.bl.bigdata.tfidf.GoodsSimilarityInCate
import com.bl.bigdata.useranalyze._

import scala.collection.mutable.ListBuffer

/**
 * Created by MK33 on 2016/4/22.
 */
class ToolManager extends Tool {

  private val list = new ListBuffer[Tool]

  override def run(args: Array[String]): Unit = {
    defaultJob(args)
    start(args)
  }

  private def defaultJob(args: Array[String])  {
    if (args.length == 0) {
      SparkFactory.getSparkContext()
      list += new BrowserGoodsSimilarity with ToolRunner // 看了又看
      list += new SeeBuyGoodsSimilarity with ToolRunner // 看了最终买
      list += new BuyGoodsSimilarity with ToolRunner // 买了还买
      list += new CategorySimilarity with ToolRunner // 品类买了还买
      list += new BuyActivityStatistic with ToolRunner // 上午 下午 晚上 购买类目
      list += new BrowserNotBuy with ToolRunner // 最近两个月浏览未购买商品 按时间排序
      list += new HotSaleGoods with ToolRunner // 品类热销商品
      list += new GoodsForSale with ToolRunner // goods for sale
      list += new UserCookie with ToolRunner // 将用户 ID 和 cookieID 导入到 redis
      list += new GoodsNewArrival with ToolRunner // 新上线商品
//      list += new GoodsSimilarityInCate with ToolRunner // 同一类别商品的相似度
//      list += new UserGoodsWeight with ToolRunner //
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
        "UserCookie".toLowerCase -> new UserCookie with ToolRunner,
        "UserGoodsWeight".toLowerCase -> new UserGoodsWeight with ToolRunner,
        "GoodsNewArrival".toLowerCase -> new GoodsNewArrival with ToolRunner,
        "als" -> new Guess with ToolRunner,
        "search" -> new Search with ToolRunner)
      for (arg <- args; key = arg.toLowerCase if map.contains(key)) list += map(key)
    }
  }

  def start(args: Array[String]) : Unit = {
    if (list.isEmpty) {
      Message.addMessage("no task.")
      return
    }
    for (tool <- list) {
      try {
        //TODO 启用子线程执行任务，主程序计时，定时查询 SparkContext 的状态，发生异常就 kill 掉，执行下一个任务。
        tool.run(args)
      } catch {
        case e: Exception =>
          Message.addMessage(s"运行出错： ${tool.getClass.getName} \n${e.getMessage}\n\n\n")
          logger.error(s"运行出错： ${tool.getClass.getName} \n${e.getMessage}")
          Thread.sleep(5 * 1000)
      }
    }
  }

  def addJob(job: Tool) = list += job

  def addJob(jobName: String): Unit = {
    //TODO 传入类的名字，调用反射动态生成Tool类
  }

}
