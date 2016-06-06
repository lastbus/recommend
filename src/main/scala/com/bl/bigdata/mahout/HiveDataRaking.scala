package com.bl.bigdata.mahout

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.{ToolRunner, ConfigurationBL, SparkFactory, Tool}
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob

/**
 *
 * Created by MK33 on 2016/6/3.
 */
class HiveDataRaking extends Tool
{

  override def run(args: Array[String]): Unit =
  {
    if (args.length < 2) {
      println("Please input <spark temp output> <output> <date count> ")
      sys.exit(-1)
    }
    import scala.sys.process._
    "00".!!


    val Array(tempOutput, output, dateCount) = args
    val start = getStartTime(dateCount)
    val sql = s"select cookie_id, behavior_type, goods_sid, dt " +
      s"from recommendation.user_behavior_raw_data " +
      s"where dt >= $start "
    val sc = SparkFactory.getSparkContext("user-item-rating")
    val rawRDD = ReadData.readHive(sc, sql)
    // 过滤得到浏览、购买、加入购物车等等操作
    val nowMills = new Date().getTime
    val ratingRDD = rawRDD.map { case Array(cookie_id, behavior_type, goods_sid, dt) =>
                                    (cookie_id, behavior_type, goods_sid, dt) }
                          .filter (s => s._2 == "1000" || s._2 == "2000" ||s._2 == "3000" || s._2 == "4000" )
                          .map { item => (item._1, item._3, getRating(item._2, item._4, nowMills)) }
    val users = ratingRDD.map(_._1).zipWithIndex
    users.map(s => s._1 + "," + s._2).saveAsTextFile("/user/spark/users")
//    val items = ratingRDD.map(_._2).zipWithIndex()
//    items.map(s => s._1 + "," + s._2).saveAsTextFile("/user/spark/items")
    val training = ratingRDD.map(s => (s._1, (s._2, s._3))).join(users).map { case (user, ((prod, rating), userIndex)) => (userIndex, prod, rating)}
//    .join(items).map { case (prod, ((userIndex, rating), prodIndex)) => (userIndex, prodIndex, rating)}
    training.map(s => s"${s._1},${s._2},${s._3}").saveAsTextFile(tempOutput)

    val job = new RecommenderJob()
    val sb = new StringBuilder()
    sb.append(" -s SIMILARITY_LOGLIKELIHOOD ")
    sb.append(s" --input $tempOutput ")
    sb.append(s" --output $output ")
    val args2 = sb.toString().split(" ").filter(_.length > 1)
    job.run(args2)

    val users1 = sc.textFile("/user/spark/users").map(s => {val a = s.split(","); (a(1), a(0))})
//    val items2 = sc.textFile("/user/spark/items").map(s => {val a = s.split(","); (a(0), a(1))})
    val hadoopResult = sc.textFile(output).map { s => val ab = s.split("\t"); (ab(0), ab(1)) }
    hadoopResult.join(users1).map { case (userIndex, (prefer, userId)) => userId + "\t" + prefer }.saveAsTextFile("result")
  }

  def getStartTime(d: String) =
  {
    val limit = d.toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
  }

  def getRating(behavior: String, date: String, now: Long) =
  {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val delt = (now - sdf.parse(date).getTime) / (24000L * 3600)
    val pow = behavior match {
      case "1000" => 1 case "2000" => 2 case "3000" => -1.5 case "4000" => 3
    }
    pow * Math.pow(0.95, delt)
  }

}

object HiveDataRaking
{

  def main(args: Array[String])
  {
    ConfigurationBL.init()
    val tool = new HiveDataRaking with ToolRunner
    tool.run(args)

  }
}
