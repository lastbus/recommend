package com.bl.bigdata.useranalyze

import com.bl.bigdata.mail.MailServer
import com.bl.bigdata.util.{HiveDataUtil, ConfigurationBL, Tool, ToolRunner}
import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 1. 统计上午、下午、晚上 购买类目top 20
  * 2. 订单里面分类目单价低于20元top20商品
  * 3. 分析订单里面购买关联类目，哪些类目同时购买
  *
  * 上午： 08-12
  * 下午：12-18
  * 晚上：其余
  * Created by MK33 on 2016/3/18.
  */
class BuyActivityStatistic extends Tool {
  private val logger = LogManager.getLogger(this.getClass.getName)
  private val message = new StringBuilder

  override def run(args: Array[String]): Unit = {
    message.clear()
    message.append("上午 下午 晚上 购买类目:\n")

    val inputPath = ConfigurationBL.get("user.behavior.raw.data")
    val outputPath = ConfigurationBL.get("recmd.output")
    val local = outputPath.contains("local")
    val redis = outputPath.contains("redis")
    val num = ConfigurationBL.get("buy.activity.category.topNum").toInt

    val sparkConf = new SparkConf().setAppName("上午 下午 晚上 购买类目")
    if (local) sparkConf.setMaster("local[*]")
    if (redis)
      for ((key, value) <- ConfigurationBL.getAll if key.startsWith("redis."))
        sparkConf.set(key, value)

    val sc = new SparkContext(sparkConf)

    val rawRDD = HiveDataUtil.read(inputPath, sc).map(line => {
      val attr = line.split("\t")
      // 类目  时间  行为编码 价格
      (attr(9),attr(6).substring(attr(6).indexOf(" ") + 1), attr(7))
    })
      // 购买记录
      .filter{ case (category, time, behavior) => behavior.equals("4000") && !category.equalsIgnoreCase("NULL")}
      .map{ case (category, time, behavior) => (category, time)}

    // 统计上午、下午、晚上 购买类目top 10
    val morning = rawRDD
      .filter{ case (category, time) => time >= "08" & time <= "12:00:00.0"}
      .map{ s => (s._1, 1)}
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(num).map(_._1).mkString("#")
    //      .map(("rcmd_topcategory_forenoon", _))

    val noon = rawRDD
      .filter{ case (category, time) => time >= "12" & time <= "18:00:00.0"}
      .map{ s => (s._1, 1)}
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(num).map(_._1).mkString("#")

    val evening = rawRDD
      .filter{ case (category, time) => time >= "18" | time <= "08"}
      .map{ s => (s._1, 1)}
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(num).map(_._1).mkString("#")

    if (redis){
      val jedis = new Jedis(ConfigurationBL.get("redis.host"), ConfigurationBL.get("redis.port", "6379").toInt)
      jedis.set("rcmd_topcategory_forenoon", morning)
      jedis.set("rcmd_topcategory_afternoon", noon)
      jedis.set("rcmd_topcategory_evening", evening)
      jedis.close()
      message.append(s"运行成功:\n上午:$morning\n中午:$noon\n晚上:$evening\n")
    }

    if (local) {
      logger.info(s"上午：$morning\n下午：$noon\n晚上：$evening")
    }
    sc.stop()
  }
}

object BuyActivityStatistic {

  def main(args: Array[String]): Unit = {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val buyActivityStatistic = new BuyActivityStatistic with ToolRunner
    buyActivityStatistic.run(args)
    MailServer.send(buyActivityStatistic.message.toString())
  }
}