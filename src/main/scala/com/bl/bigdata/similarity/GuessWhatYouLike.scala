package com.bl.bigdata.similarity


import com.bl.bigdata.util.{PropertyUtil, ToolRunner, Tool}
import java.text.SimpleDateFormat
import java.util.{Date, NoSuchElementException}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool
import com.bl.bigdata.util.RedisUtil._

/**
 * Created by blemall on 3/23/16.
 */

class GuessWhatYouLike extends Tool {
  val attenuationRatio = PropertyUtil.get("gueswhatyoulike.attenuation.ratio").toDouble
  val effectiveDay = PropertyUtil.get("gueswhatyoulike.effective.day").toInt
  val rank = PropertyUtil.get("gueswhatyoulike.rank").toInt
  val lambda = PropertyUtil.get("gueswhatyoulike.lambda").toDouble
  val numIter = PropertyUtil.get("gueswhatyoulike.number.iterator").toInt
  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  override def run(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 1) {
      println("spark-submit --master local[*] --class com.bailian.bigdata.similarity.GuessWhatYouLike recommend-1.0-SNAPSHOT.jar " +
        "user_behavior_raw_data")
      sys.exit(1)
    }

    // set up environment
    val conf = new SparkConf()
      .setAppName("GuessWhatYouLike")
      .set("spark.executor.memory", "5g")
      .setMaster("local[*]") //only for local test
    val sc = new SparkContext(conf)
    val ratingsFilePath = args(0).trim
    val ratingsCache =sc.textFile(ratingsFilePath).map { line =>
      val fields = line.split("\t")
      //(cookieId, goodsId, goodsName, behaviorType, dt)
      (fields(0), fields(3).toInt, fields(4), fields(7).toInt, fields(10))
    }.cache()

    val ratings = ratingsCache.map{x =>
        ((x._1, x._2, x._3, x._5), x._4 match {
          case 1000 => 1
          case 2000 => 2
          case 3000 => -1
          case 4000 => 3
        })
    }.reduceByKey(_ + _).map{v =>

      (v._1._1, v._1._2, v._2 * calc(v._1._4.toString))
    }
    var index = 0
    //(cookieId, index)
    val cookieIdMap = ratings.map(_._1).distinct().map{
      x =>
        index = index + 1
        (x, index)
    }.collectAsMap()

    //with index (index, goodsId, score)
    val replacedRatings = ratings.map{x => Rating(cookieIdMap(x._1), x._2.toInt, x._3.toDouble)}
    val model = ALS.train(replacedRatings, rank, numIter, lambda)

    val result = cookieIdMap.map{ x =>
      var r: Array[Rating] = Array()
      try {
         r = model.recommendProducts(x._2, 20)
      }catch {
        case ex:NoSuchElementException =>
      }
      (x._1, r)
    }.toMap
    print("result ================ " + result.size)
    val jedisPool = getJedisPool
    saveToRedis(conf, jedisPool, result)

    // clean up*/
    sc.stop()
  }
  def saveToRedis(sparkConf: SparkConf ,jedisPool: JedisPool, values: Map[String, Array[Rating]]): Unit = {
    sparkConf.set("redis.host", "10.201.128.216")
    sparkConf.set("redis.port", "6379")
    sparkConf.set("redis.timeout", "10000")
    val jedis = jedisPool.getResource
    import com.bl.bigdata.util.Implicts.map2HashMap
    values.map{v =>
      val map = v._2.map{r => (r.product.toString, r.rating.toString)}.distinct.toMap
      if(map.nonEmpty) {
        jedis.hmset("rcmd_gwyl_" + v._1.toString, map)
      }
    }
    println("finished saving data to redis")
  }

  def calc(day: String): Double = {
    val now = (new Date).getTime
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val d = dateFormat.parse(day)
    val n = this.effectiveDay - (now - d.getTime)/(24 * 60 * 60 * 1000)
    if (n == 0 || n < 0 ) 1.0 else math.pow(attenuationRatio, n)
  }
}

object GuessWhatYouLike {
  def main(args: Array[String]) {
    (new GuessWhatYouLike with ToolRunner).run(args)
  }
}
