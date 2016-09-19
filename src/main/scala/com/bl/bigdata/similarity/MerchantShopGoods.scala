package com.bl.bigdata.similarity

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util.{DataBaseUtil, MyCommandLine, RedisClient, Tool}

/**
  * 统计商家店铺里的所有商品的数据
  * Created by MK33 on 2016/7/29.
  */
class MerchantShopGoods extends Tool {

  override def run(args: Array[String]): Unit = {
    logger.info("merchant shop goods begin to run.")
    Message.addMessage("\n Merchant shop goods : ")
    val optionMap = try {
      MerchantShopGoodsConf.parse(args)
    } catch {
      case e: Throwable =>
        logger.error("commandline parser error: " + e)
        MerchantShopGoodsConf.printlnHelp
        return
    }

    val input = optionMap(MerchantShopGoodsConf.input)
    val output = optionMap(MerchantShopGoodsConf.output)
    val sqlName = optionMap(MerchantShopGoodsConf.sqlName)
    val prefix = optionMap(MerchantShopGoodsConf.prefix)
    val delimiter = optionMap(MerchantShopGoodsConf.delimiter)

    val sql = " select sid, store_id  from recommendation.goods_avaialbe_for_sale_channel where store_sid <> 'null' and isnotnull(null) "

    val rawData = DataBaseUtil.getData(input, sqlName)

    val shopGoodsRDD = rawData.filter(!_.contains("null")).map { case Array(goodsId, storeId) => (storeId, Seq(goodsId)) }
                        .reduceByKey(_ ++ _).map { case (storeId, goods) => (prefix + storeId, goods.distinct.mkString(delimiter)) }


    if (output.contains("redis")) {
      // save to redis cluster or standalone, default is redis cluster
      val redisType = if (output.contains(RedisClient.cluster)) RedisClient.cluster else RedisClient.standalone
      val accumulator = shopGoodsRDD.sparkContext.accumulator(0)
      RedisClient.sparkKVToRedis(shopGoodsRDD, accumulator, redisType)
      Message.addMessage(s" insert into redis  ${accumulator.value}")

    }


    logger.info("merchant goods run finished.")

  }
}

object MerchantShopGoods {

  def main(args: Array[String]) {
    val jobs = new MerchantShopGoods
    jobs.run(args)
  }
}

object MerchantShopGoodsConf  {

  val input = "input"
  val output = "output"
  val sqlName = "sql"
  val prefix = "prefix"
  val delimiter = "delimiter"

  val commandLineParser = new MyCommandLine("MerchantShopGoods")
  commandLineParser.addOption("i", input, true, "input data source", "hive")
  commandLineParser.addOption("o", output, true, "output data destination", "redis-" + RedisClient.cluster)
  commandLineParser.addOption("s", sqlName, true, "sql name in hive.xml file", "merchant.shop.goods")
  commandLineParser.addOption("p", prefix, true, "redis key prefix", "rcmd_store_")
  commandLineParser.addOption("d", delimiter, true, "goods_id delimiter", "#")

  def parse(args: Array[String]): Map[String, String] = commandLineParser.parser(args)

  def printlnHelp = commandLineParser.printHelper

}