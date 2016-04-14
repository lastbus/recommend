package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.{Date, NoSuchElementException}

import com.bl.bigdata.SparkEnv
import com.bl.bigdata.datasource.{Item, ReadData}
import com.bl.bigdata.util._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{Accumulator, SparkConf}
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Protocol}

import scala.collection.JavaConversions._


/**
  * Created by blemall on 3/23/16.
  */

class GuessWhatYouLike extends Tool with SparkEnv{
    val attenuationRatio = PropertyUtil.get("guesswhatyoulike.attenuation.ratio").toDouble
    val effectiveDay = PropertyUtil.get("guesswhatyoulike.effective.day").toInt
    val rank = PropertyUtil.get("guesswhatyoulike.rank").toInt
    val lambda = PropertyUtil.get("guesswhatyoulike.lambda").toDouble
    val numIter = PropertyUtil.get("guesswhatyoulike.number.iterator").toInt

    override def run(args: Array[String]): Unit = {
        val sc = SparkFactory.getSparkContext
        val limit = ConfigurationBL.get("day.before.today", "90").toInt
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val date = new Date
        val start = sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
        val sql = s"select cookie_id, behavior_type, goods_sid, goods_name, dt from recommendation.user_behavior_raw_data where dt >= $start "
        val ratingsCache = ReadData.readHive(sc, sql)
          .map{case Item(Array(cookie, behavior, goodsID, goodsName, dt)) => (cookie, goodsID, goodsName, behavior, dt)}
          .cache()
//        val ratingsFilePath = args(0).trim
//        val ratingsCache =sc.textFile(ratingsFilePath).map { line =>
//            val fields = line.split("\t")
//            //(cookieId, goodsId, goodsName, behaviorType, dt)
//            (fields(0), fields(3).toInt, fields(4), fields(7), fields(10))
//        }.cache()

        val ratings = ratingsCache.filter(_._4 != "0000").map{x =>
            ((x._1, x._2, x._3, x._5), x._4 match {
                case "1000" => 1
                case "2000" => 2
                case "3000" => -1
                case "4000" => 3
            })
        }.reduceByKey(_ + _).map{v =>
            (v._1._1, v._1._2, v._2 * calc(v._1._4.toString))
        }
        var index = 0
        //(cookieId, index)
        val cookieIdMap = ratings.map(_._1).distinct().zipWithUniqueId().map(s =>(s._1, s._2.toInt)).collectAsMap()

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

        RedisUtil.guessWhatYouLike_saveToRedis(new SparkConf(), result)

        // clean up*/
        sc.stop()
    }

    /** Compute RMSE (Root Mean Squared Error). */
    def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
        val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
        val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
                .join(data.map(x => ((x.user, x.product), x.rating)))
                .values
        math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
    }


  def calc(day: String): Double = {
    val now = (new Date).getTime
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val d = dateFormat.parse(day)
    val n = this.effectiveDay - (now - d.getTime)/(24 * 60 * 60 * 1000)
    if (n == 0 || n < 0 ) 1.0 else math.pow(attenuationRatio, n)
  }

  def getJedisPool = {
    new JedisPool(new JedisPoolConfig, "", 6379, Protocol.DEFAULT_TIMEOUT, "") with Serializable
  }

    def saveToRedis(rdd: RDD[Map[String, Array[Rating]]], accumulator: Accumulator[Int]): Unit = {
        rdd.foreachPartition(partition => {
            val jedis = RedisClient.pool.getResource
            partition.foreach(s => {
                accumulator += 1
                s.foreach(map => jedis.hmset("rcmd_gwyl_" + map._1,
                    map._2.map(r=>(r.product.toString, r.rating.toString)).toMap))
            })
            jedis.close()
        })
    }

}

object GuessWhatYouLike {
    def main(args: Array[String]) {
        (new GuessWhatYouLike with ToolRunner).run(args)
    }
}