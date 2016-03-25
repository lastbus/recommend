package com.bl.bigdata.similarity

import com.bl.bigdata.util.{ToolRunner, Tool}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool
import scala.collection.immutable.HashMap
import com.bl.bigdata.util.RedisUtil._

/**
 * Created by blemall on 3/23/16.
 */
class GuessWhatYouLike extends Tool {

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  def saveToRedis(sparkConf: SparkConf ,jedisPool: JedisPool, values: Map[Integer, Array[Rating]]): Unit = {
    sparkConf.set("redis.host", "10.201.128.216")
    sparkConf.set("redis.port", "6379")
    sparkConf.set("redis.timeout", "10000")
    val jedis = jedisPool.getResource
    import com.bl.bigdata.util.implicts.map2HashMap
    values.map{v =>
      val map = v._2.map{r => (r.product.toString, r.rating.toString)}.distinct.toMap
      jedis.hmset(v._1.toString, map)
    }
  }

  override def run(args: Array[String]): Unit = {
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
    // load ratings and movie titles

    val ratingsFilePath = args(0).trim
    val ratingsCache =sc.textFile(ratingsFilePath).map { line =>
      val fields = line.split("\t")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(0), fields(1).toInt, fields(2).toInt, fields(3).toDouble, fields(4).toLong)
    }.cache()
    val ratings = ratingsCache.map{x => (x._5 % 10, Rating(x._2, x._3, x._4))}
    val userIdMap = ratingsCache.map(x => (x._3, x._2))
    val numRatings = ratings.count()
    val users = ratings.map(_._2.user).distinct()
    val numUsers = users.count()
    val numProducts = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numProducts + " products.")

    // split ratings into train (60%), validation (20%), and test (20%) based on the
    // last digit of the timestamp, add myRatings to train, and cache them

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

    // train models and evaluate them on the validation set

    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    // evaluate the best model on the test set
    val testRmse = computeRmse(bestModel.get, test, numTest)
    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model

    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
    //loop on bestModel and input data to redis
    val result: Map[Integer, Array[Rating]] = new HashMap[Integer, Array[Rating]]()
    val model = bestModel.get
    userIdMap.map{ x =>
      val v = model.recommendProducts(x._2, 20)
      result.+((x._1, v))
    }
    val jedisPool = getJedisPool
    saveToRedis(conf, jedisPool, result)

    // clean up
    sc.stop()
  }
}

object GuessWhatYouLike {

  def main(args: Array[String]) {
    (new GuessWhatYouLike with ToolRunner).run(args)
  }
}
