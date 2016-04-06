package com.bl.bigdata

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
    // 看了又看
    BrowserGoodsSimilarity.execute(args)
    // 看了最终买
    SeeBuyGoodsSimilarity.execute(args)
    // 买了还买
    BuyGoodsSimilarity.execute(args)
    // 品类买了还买
    CategorySimilarity.execute(args)
    // 上午 下午 晚上 购买类目
    BuyActivityStatistic.execute(args)
    // 最近两个月浏览未购买商品 按时间排序
    BrowserNotBuy.execute(args)
    // 品类热销商品
    HotSaleGoods.execute(args)
  }
}
