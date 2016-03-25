package com.bl.bigdata.similarity

import java.util.NoSuchElementException

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool
import com.bl.bigdata.util.RedisUtil._

/**
 * Created by blemall on 3/23/16.
 */
object GuessWhatYouLike {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length != 1) {
      println("spark-submit --master local[*] --class com.bailian.bigdata.similarity.GuessWhatYouLike recommend-1.0-SNAPSHOT.jar " +
        "user_product_score")
      sys.exit(1)
    }

    // set up environment
    val conf = new SparkConf()
      .setAppName("GuessWhatYouLike")
      .set("spark.executor.memory", "2g")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ratingsFilePath = args(0).trim
    val ratingsCache =sc.textFile(ratingsFilePath).map { line =>
      val fields = line.split("\t")
      (fields(0), fields(1).toInt, fields(2).toInt, fields(3).toDouble, fields(4).toLong)
    }.cache()
    val ratings = ratingsCache.map{x => (x._5 % 10, Rating(x._2, x._3, x._4))}
    val userIdMap = ratingsCache.map(x => (x._2, x._1)).collect().toMap
    val numRatings = ratings.count()
    val users = ratings.map(_._2.user).distinct()
    val numUsers = users.count()
    val numProducts = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numProducts + " products.")
    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6)
      .values
      .repartition(numPartitions)
      .cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)
    val rank = 12
    val lambda = 0.1
    val numIter = 20
    val model = ALS.train(training, rank, numIter, lambda)

    val result = userIdMap.map{ x =>
      var r: Array[Rating] = Array()
      try {
         r = model.recommendProducts(x._1, 20)
      }catch {
        case ex:NoSuchElementException =>
      }
      (x._2, r)
    }.toMap
    val jedisPool = getJedisPool
    saveToRedis(conf, jedisPool, result)

    // clean up
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
}
