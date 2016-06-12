package com.bl.bigdata.product

import com.bl.bigdata.accumulator.MyAccumulator._
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkContext}


/**
  * 计算商品的属性，保存成redis中hash值。
  * Created by MK33 on 2016/3/21.
  */
class GoodsForSale extends Tool {

  override def run(args: Array[String]): Unit = {
    logger.info("商品属性开始计算......")
    Message.addMessage("\ngoods for sale:\n")
    val sc = SparkFactory.getSparkContext("goods.for.sale")
    val hiveContext = new HiveContext(sc)
    toRedis1(hiveContext)
    toRedis2(hiveContext)
    category(hiveContext)

    logger.info("商品属性计算结束。")
  }

  /** 商品类别下面有哪些商品，某个商品属于那个类别 */
  def toRedis1(hiveContext: HiveContext) = {
    logger.info("\t商品类别下面有哪些商品开始计算....")
    val prefix = ConfigurationBL.get("goods.for.sale")
    val prefix2 = ConfigurationBL.get("goods.for.sale.2")
    val sql = "select sid, category_id from recommendation.goods_avaialbe_for_sale_channel where channel_sid = '3'"
    val readRDD = hiveContext.sql(sql).rdd.map(row => if (row.anyNull) null else (row.getString(0), row.getLong(1).toString))
                                          .filter(_ != null).distinct()
    val sc = hiveContext.sparkContext
    val count1Accumulator = sc.accumulator(0)
    val count2Accumulator = sc.accumulator(0)
    val count3Accumulator = sc.accumulator(0)
    val count4Accumulator = sc.accumulator(0)
    val goodsIDToCategoryIDRDD = readRDD map { case (goodsID, categoryID) =>
      count1Accumulator += 1
      (prefix + goodsID, categoryID)
    }
//    sc.toRedisKV(goodsIDToCategoryIDRDD)
    saveToRedis(goodsIDToCategoryIDRDD, count2Accumulator)
    Message.addMessage(s"\t$prefix* : $count1Accumulator\n")
    Message.addMessage(s"\t插入 redis $prefix* : $count2Accumulator\n")

    val categoryIDToGoodsID = readRDD.map(s => (s._2, Seq(s._1))).reduceByKey(_ ++ _)
                                      .map { case (categoryID, goodsID) =>
                                        count3Accumulator += 1
                                        (prefix2 + categoryID, goodsID.mkString("#"))}
//    sc.toRedisKV(categoryIDToGoodsID)
    saveToRedis(categoryIDToGoodsID, count4Accumulator)
    Message.addMessage(s"\t插入 $prefix2*: $count3Accumulator\n")
    Message.addMessage(s"\t插入 $prefix2*: $count4Accumulator\n")
    logger.info("\t商品类别下面有哪些商品计算结束。")
  }

  /**
    * 统计商品属性，保存在 redis 中。
    * @param hiveContext 读取 hive 表
    */
  def toRedis2(hiveContext: HiveContext) = {


    val sql = "select sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, " +
                      "cn_name, category_id, category_name, sale_price, pic_sid, url, channel_sid  " +
              "from recommendation.goods_avaialbe_for_sale_channel where channel_sid == '3' or channel_sid = '1' "
    val sc = hiveContext.sparkContext
    val accumulator = sc.accumulator(0)
    val accumulator1 = sc.accumulator(0)
    val errorMessage = sc.accumulator(new StringBuffer())
    val prefix = ConfigurationBL.get("goods.attr")
    val r = hiveContext.sql(sql).rdd
                        .map(row => if (row.isNullAt(7)) null else (row.getString(0), row.getString(1), row.getString(2),
                                row.getDouble(3).toString, row.getString(4), row.getString(5),
                                row.getString(6), row.getLong(7).toString, row.getString(8),
                                row.getDouble(9).toString, row.getString(10), row.getString(11),
                                row.getString(12)))
                        .filter(_ != null)
                        .distinct()
                        .map{ case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
                                      category_id, category_name, sale_price, pic_id, url, channel) =>
                                            val map = Map("sid" -> sid, "mdm_goods_sid" -> mdm_goods_sid,
                                                          "goods_sales_name" -> goods_sales_name, "goods_type" -> goods_type,
                                                          "pro_sid" -> pro_sid, "brand_sid" -> brand_sid,
                                                          "cn_name" -> cn_name, "category_id" -> category_id,
                                                          "category_name" -> category_name, "sale_price" -> sale_price,
                                                          "pic_sid" -> pic_id, "url" -> url)
                                            val m = new java.util.HashMap[String, String]
                                            for ((k, v) <- map) if (v != null) m.put(k, v) else m.put(k, "null")
                                            val terminal = if ( channel == "3") "pc_" else "app_"
                                            (prefix + terminal + sid, m)}
//    sc.hashKVRDD2Redis(r)
//    saveToRedisHash(r, accumulator, accumulator1)
    saveToRedisHash2(r, accumulator, accumulator1, errorMessage)
//    val result = RedisWriter.saveHashValue(r)
    if (errorMessage.value.length() > 1) Message.addMessage(errorMessage.value.toString)
    Message.addMessage(s"\t$prefix*: $accumulator\n")
    Message.addMessage(s"\t插入 redis $prefix pc/app*: $accumulator1\n")
//    logger.info(result)
//    Message.addMessage(result)
  }

  /**
   * 搜集executor失败的信息，返回到driver
   * @param rdd
   * @param accumulator
   * @param accumulator2
   */
  def saveToRedisHash2(rdd: RDD[(String, java.util.HashMap[String, String])],
                      accumulator: Accumulator[Int], accumulator2: Accumulator[Int],
                        messageAccumulator: Accumulator[StringBuffer]) ={
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      var i = 0
      val sb = new StringBuffer()
      partition.foreach(data => {
        accumulator += 1
        try {
          jedis.hmset(data._1, data._2)
        } catch {
          case e: Exception =>
            i += 1
            sb.append(s"insert into redis error : ${data._1}\n ${e.getMessage}\n")
            if (i > 10) throw new Exception(e) // 累计导入redis失败100条则报错。
        }
        accumulator2 += 1
        messageAccumulator += sb
      })
      jedis.close()

    })
  }

  def saveToRedisHash(rdd: RDD[(String, java.util.HashMap[String, String])],
                      accumulator: Accumulator[Int], accumulator2: Accumulator[Int]) ={
    rdd.foreachPartition(partition => {
      try {
        val jedis = RedisClient.pool.getResource
        partition.foreach(data => {
          accumulator += 1
          jedis.hmset(data._1, data._2)
          accumulator2 += 1
        })
        jedis.close()
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }

    })
  }

  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      try {
        val jedis = RedisClient.pool.getResource
        partition.foreach(s => {
          jedis.set(s._1, s._2)
          accumulator += 1
        })
        jedis.close()
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
  }


  /**
    * 根据商品 id 得到它上一级品类ID；
    * 根据品类ID，得到所属的商品ID列表
    * @param hiveContext 读取 hive 表
    */
  def category(hiveContext: HiveContext): Unit = {
    val sc = hiveContext.sparkContext
    val accumulator1 = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)
    val sql = "select category_id, level2_id, level3_id from recommendation.dim_category"
    val readRDD = hiveContext.sql(sql).rdd.map(row =>
                                      if (!row.anyNull) (row.getLong(0).toString, row.getLong(1).toString, row.getLong(2).toString) else null)
                              .filter(_ != null)
                              .map{ case (goodsID, parents, sub) => (if(goodsID == sub) parents else sub, Seq(goodsID))}
                              .distinct().reduceByKey(_ ++ _)
                              .map { case (parent, children) =>
                                          val size = children.length
                                          val array = new Array[(String, String)](size + 1)
                                          for (i <- 0 until size)
                                            array(i) = ("rcmd_parent_category_" + children(i), parent)
                                          array(size) = ("rcmd_subcategory_" + parent, children.mkString("#"))
                                          accumulator1 += 2
                                          array }
                              .flatMap(s => s)
//    sc.toRedisKV(readRDD)
    saveToRedis(readRDD, accumulator2)
    Message.addMessage(s"\t rcmd_*category_*: $accumulator1\n")
    Message.addMessage(s"\t插入 redis rcmd_*category_*: $accumulator2\n")
  }

}

object GoodsForSale {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val test = new GoodsForSale with ToolRunner
    test.run(args)
  }

  def sparkShell(sc: SparkContext) = {

    import org.apache.commons.pool2.impl.GenericObjectPoolConfig
    import org.apache.spark.sql.hive.HiveContext
    import redis.clients.jedis.JedisPool

    val sql = "select sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, " +
      "cn_name, category_id, category_name, sale_price, pic_sid, url, channel_sid  " +
      "from recommendation.goods_avaialbe_for_sale_channel where channel_sid == '3' or channel_sid = '1' "
    val hiveContext = new HiveContext(sc)

    val prefix = "rcmd_orig_"
    val r = hiveContext.sql(sql).rdd
      .map(row => if (row.isNullAt(7)) null else (row.getString(0), row.getString(1), row.getString(2),
        row.getDouble(3).toString, row.getString(4), row.getString(5),
        row.getString(6), row.getLong(7).toString, row.getString(8),
        row.getDouble(9).toString, row.getString(10), row.getString(11),
        row.getString(12)))
      .filter(_ != null)
      .distinct()
      .map{ case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
      category_id, category_name, sale_price, pic_id, url, channel) =>
        val map = Map("sid" -> sid, "mdm_goods_sid" -> mdm_goods_sid,
          "goods_sales_name" -> goods_sales_name, "goods_type" -> goods_type,
          "pro_sid" -> pro_sid, "brand_sid" -> brand_sid,
          "cn_name" -> cn_name, "category_id" -> category_id,
          "category_name" -> category_name, "sale_price" -> sale_price,
          "pic_sid" -> pic_id, "url" -> url)
        val m = new java.util.HashMap[String, String]
        for ((k, v) <- map) m.put(k, v)
        val terminal = if ( channel == "3") "pc_" else "app_"
        (prefix + terminal + sid, m)}

    r.foreachPartition(partition => {
      try {
        val conf = new GenericObjectPoolConfig
        conf.setMaxTotal(100)
        val jedis = new JedisPool(conf, "10.201.48.13", 6379, 5000).getResource
        partition.foreach(s => {
          jedis.hmset(s._1, s._2)
        })
        jedis.close()
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })


  }
}
