package com.bl.bigdata.useranalyze

import java.text.SimpleDateFormat
import java.util.{Date, NoSuchElementException}

import com.bl.bigdata.datasource.{Item, ReadData}
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}

/**
  * Created by MK33 on 2016/4/12.
  */
class Guess extends Tool{

  val cookiePath = "/user/als/cookie"
  val modelPath = "/user/als/model"

  override def run(args: Array[String]): Unit = {

    val sc = SparkFactory.getSparkContext("ALS model")
    val model = trainModel(sc)
    val r = ReadData.readLocal(sc, cookiePath).collect()
      .map { case Item(Array(cookie, index)) =>(cookie, index)}.toMap.
      map{ case (cookie, index) =>
        val t = try {
          model.recommendProducts(index.toInt, 20)
        }catch {
          case ex:NoSuchElementException =>
            Array(Rating(0, 0, 0.0))
        }
      ("rcmd_guess_" + cookie, t.map(_.product.toString).mkString("#"))}

    val count = sc.accumulator(0)
    saveToRedis(sc.parallelize(r.toSeq), count)
    Message.sendMail
    SparkFactory.destroyResource()
  }


  def hh(train: Boolean, pred: Boolean, sc: SparkContext): RDD[(String, String)] = {
    val model = if (train) trainModel(sc) else MatrixFactorizationModel.load(sc, "")
    val r: RDD[(String, String)] =
      if (pred && train)
        ReadData.readLocal(sc, cookiePath).map { case Item(Array(cookie, index)) =>
          val t = try {
            model.recommendProducts(index.toInt, 20)
          }catch {
            case ex:NoSuchElementException => null
          }
          (cookie, t.map(_.product.toString).mkString("#"))
        }
      else null
    r
  }

  def trainModel(sc: SparkContext): MatrixFactorizationModel = {
    val start = getStartTime
    val sql = s"select cookie_id, behavior_type, goods_sid, dt " +
      s"from recommendation.user_behavior_raw_data " +
      s"where dt >= $start"
    val rawRDD = ReadData.readHive(sc, sql)

    val ratingRDD = rawRDD.map{ case Item(Array(cookie_id, behavior_type, goods_sid, dt)) =>
      (cookie_id, (behavior_type, goods_sid, dt)) }

    val nowMills = new Date().getTime
    val cookieIndex = ratingRDD.map(_._1).distinct().zipWithUniqueId()
    cookieIndex.cache()

    val fs = FileSystem.get(new Configuration())
    if ( fs.exists(new Path(cookiePath))) fs.delete(new Path(cookiePath), true)
    cookieIndex.map( s => s._1 + "\t" + s._2 ).saveAsTextFile(cookiePath)
    Message.addMessage(s"save cookie to path: $cookiePath")
    Message.sendMail


    val trainRDD = ratingRDD.join(cookieIndex).filter(s =>
        s._2._1._1 == "1000" || s._2._1._1 == "2000" ||s._2._1._1 == "3000" || s._2._1._1 == "4000" )
      .map{ case (cookie, ((behavior, goods, dt), index)) =>
      ((index.toInt, goods.toInt), getRating(behavior, dt, nowMills)) }.reduceByKey(_ + _)
      .map{ case ((index, goods), rating) => Rating(index, goods, rating)}
    cookieIndex.unpersist()

    trainRDD.cache()
    Message.addMessage("trainRDD count: " + trainRDD.count())
    Message.sendMail
    val model = ALS.train(trainRDD, 1, 1, 0.01)

    val usersProducts = trainRDD.map{ case Rating(user, product, rating) => (user, product) }

    val predict = model.predict(usersProducts).map{ case Rating(user, product, rate) => ((user, product), rate)}

    val ratesAndPredicts = trainRDD.map{ case Rating(user, product, rating) => ((user, product), rating) }
        .join(predict)

    val MSE = ratesAndPredicts.map{case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()

    Message.message.append(s"ALS Mean Squared Error =  $MSE \n\n")

    if (fs.exists(new Path(modelPath))) fs.delete(new Path(modelPath), true)
    fs.close()
    model.save(sc, modelPath)
    Message.addMessage(s"save als model to $modelPath \n\n")
    Message.sendMail
    model
  }

  def hasFile(file: String): Boolean ={
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(file))
  }
  def getRating(behavior: String, date: String, now: Long): Double = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val delt = (now - sdf.parse(date).getTime) / (24000L * 3600)
    val pow = behavior match {
      case "1000" => 1 case "2000" => 2 case "3000" => -1.5 case "4000" => 3
    }
    pow * Math.pow(0.95, delt)
  }

  def getStartTime: String = {
    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
  }


  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        accumulator += 1
        jedis.set(s._1, s._2)
      })
      jedis.close()
    })
  }

}

object Guess {

  def main(args: Array[String]) {
    val guess = new Guess with ToolRunner
    guess.run(args)

  }
}
