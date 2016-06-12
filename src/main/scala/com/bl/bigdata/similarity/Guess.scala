package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.JavaConversions._

/**
 *
 * 协同过滤算法：
 * 算法实现很简单，目前遇到的问题：数据量大就会出错。
 *
  * Created by MK33 on 2016/4/12.
  */
class Guess extends Tool {


  override def run(args: Array[String]): Unit = {
    logger.info("ALS 模型开始计算......")
    val sc = SparkFactory.getSparkContext("ALSModel")
    trainModel(sc)
    logger.info("ALS 模型计算结束。")
  }

  def trainModel(sc: SparkContext)  = {
    /** 中间结果的保存路径 */
    val cookiePath = ConfigurationBL.get("als.cookie.path")
    val productPath = ConfigurationBL.get("als.product.path")
    val modelPath = ConfigurationBL.get("als.model.path")

    val start = getStartTime
    val sql = s"select cookie_id, behavior_type, goods_sid, dt " +
      s"from recommendation.user_behavior_raw_data " +
      s"where dt >= $start "
    val rawRDD = ReadData.readHive(sc, sql)
    val sparkConf = sc.getConf
    val executorNum = sparkConf.get("spark.executor.instances", "2").toInt // executor 的个数
    val executorCores = sparkConf.get("spark.executor.cores", "1").toInt //yarn 模式下默认为1，其他模式不一定
    val executorMemPer = getMemory(sparkConf.get("spark.executor.memory", "1g")) //executor 的内存
    val totalMemory = executorNum * executorMemPer

    // 过滤得到浏览、购买、加入购物车等等操作
    val ratingRDD = rawRDD.coalesce(executorCores * executorNum, shuffle = true)
                          .map { case Array(cookie_id, behavior_type, goods_sid, dt) =>
                            (cookie_id, (behavior_type, goods_sid, dt)) }
                          .filter (s => s._2._1 == "1000" || s._2._1 == "2000" ||s._2._1 == "3000" || s._2._1 == "4000" )
    ratingRDD.persist()
    val nowMills = new Date().getTime
    val cookieIndex = ratingRDD.map(_._1).distinct().zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK)
    val cookieNum = cookieIndex.count()
    val productIndex = ratingRDD.map(_._2._2).distinct().zipWithIndex().persist(StorageLevel.MEMORY_AND_DISK)
    val productNum = productIndex.count()

    Message.addMessage(s"There are $cookieNum  users , and $productNum  products.")

    /** 有个问题，从 hive 读入的数据是大量的小文件，有好几千，需要把这些小文件合并成大文件，那么合并的依据是什么？
      * 初步做法是： executor.instances * executor.cores 。
      *
      * */
    val fs = FileSystem.get(new Configuration())
    if ( fs.exists(new Path(cookiePath))) fs.delete(new Path(cookiePath), true)
    cookieIndex.map( s => s._1 + "\t" + s._2 ).saveAsTextFile(cookiePath)
    if ( fs.exists(new Path(productPath)) ) fs.delete(new Path(productPath), true)
    productIndex.map( s => s._1 + "\t" + s._2).saveAsTextFile(productPath)
    Message.addMessage(s"save cookie and product to path:\t $cookiePath \t\t$productPath")

    /** 这儿用 left join 效果会不会更好一点？ */
    val tempRDD = ratingRDD.join(cookieIndex).map { case (cookie, ((behavior, goods_sid, dt), userIndex)) =>
                                                          (goods_sid, (behavior, userIndex, dt)) }
    val trainRDD = tempRDD.join(productIndex).map { case (goods_sid, ((behavior, userIndex, dt), productIndex0)) =>
                                                          ((userIndex.toInt, productIndex0.toInt), getRating(behavior, dt, nowMills)) }
                                              .reduceByKey(_ + _)
                                              .map { case ((userIndex, productIndex0), rating) =>
                                                           Rating(userIndex, productIndex0, rating) }
    trainRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    Message.addMessage("trainRDD count:\t" + trainRDD.count())

    val rank = ConfigurationBL.get("als.rank", "10").toInt
    val iterator = ConfigurationBL.get("als.iterator", "10").toInt
    val model = ALS.train(trainRDD, rank, iterator)
    // 计算模型的均方差
    val usersProducts = trainRDD.map { case Rating(user, product, rating) => (user, product) }
    val predict = model.predict(usersProducts).map{ case Rating(user, product, rate) => ((user, product), rate)}
    val ratesAndPredicts = trainRDD.map { case Rating(user, product, rating) => ((user, product), rating) }
        .join(predict)
    val MSE = ratesAndPredicts.map { case ((user, product), (r1, r2)) =>
                                      val err = r1 - r2
                                      err * err}.mean()

    Message.addMessage(s"ALS Mean Squared Error =\t$MSE \n")
    if (fs.exists(new Path(modelPath))) fs.delete(new Path(modelPath), true)
    fs.close()
    model.save(sc, modelPath)
    Message.addMessage(s"save als model to: \t$modelPath\n")
    // 计算 user-item 矩阵的大小
    val memoryNeed = 24L * cookieNum * productNum / 1024 / 1024 / 1024
    if (memoryNeed.toDouble / totalMemory > 1)
      Message.addMessage(s"total memory : $totalMemory, but needs:  $memoryNeed ")

    val strategy = ConfigurationBL.get("als.strategy", "matrix")
    strategy match {
      case "matrix" => sparkMatrix(cookieIndex, productIndex, model)
      case "cartestian" => cartesian(cookieIndex, productIndex, model)
      case "recommend" => recommend(cookieIndex, productIndex, model)
      case _ => throw new Exception("please input [matrix] or [cartestian]")
    }

  }

  /** 通过 spark 矩阵去计算 user-item 矩阵，数据量一大就会报错。*/
  def sparkMatrix(cookieIndex: RDD[(String, Long)], productIndex: RDD[(String, Long)], model: MatrixFactorizationModel): Unit = {
    val takeNum = ConfigurationBL.get("als.take.num", "30").toInt
    val initValue = new Array[(Long, Double)](takeNum).map(s => (-1L, 0.0))
    val aggregate = (init: Array[(Long, Double)], toBeAdd: (Long, Double)) => insertSort2(init, toBeAdd)
    val combine = (op1: Array[(Long, Double)], op2: Array[(Long, Double)]) => {{for (v <- op2) insertSort2(op1, v);op1}}

    val user = model.userFeatures.map { case (userIndex, rating) => for (r <- 0 until rating.length - 1) yield MatrixEntry(userIndex, r, rating(r))}.flatMap(s => s)

    val userBlockMatrix = new CoordinateMatrix(user).toBlockMatrix()
    //    userBlockMatrix.persist(StorageLevel.MEMORY_AND_DISK)  //这个矩阵不大，其实不需要缓存
    val product = model.productFeatures.map { case (productIndex0, rating) =>
      for (p <- 0 until rating.length - 1) yield MatrixEntry(productIndex0, p, rating(p))
    }
      .flatMap(s => s)
    val productBlockMatrix = new CoordinateMatrix(product).transpose().toBlockMatrix()
    val cookieProductMatrix = userBlockMatrix.multiply(productBlockMatrix).toCoordinateMatrix().entries
    //    cookieProductMatrix.persist(StorageLevel.MEMORY_AND_DISK)
    val aggregateUserItemMatrix = cookieProductMatrix.map { case MatrixEntry(user0, product0, rating) => (user0, (product0, rating)) }
      .aggregateByKey(initValue)(aggregate, combine)
    aggregateUserItemMatrix.persist(StorageLevel.MEMORY_AND_DISK_SER) // 序列化以后缓存在磁盘和内存中

    val productItemTuple = aggregateUserItemMatrix.map { case (user0, products) =>
      for ((productIndex, rating) <- products) yield (productIndex, (user0, rating))
    }
      .flatMap(s => s)
    val replaceProduct = productIndex.map(s => (s._2, s._1))
      .join(productItemTuple)
      .map { case (productIndex0, (product0, (user0, rating))) => (user0, (product0, rating)) }

    //    replaceProduct.checkpoint() //可以考虑把这个结果 checkpoint 起来
    val replaceItemIndex = cookieIndex.map(s => (s._2, s._1))
                                      .join(replaceProduct)
                                      .map { case (userIndex, (user0, (product0, rating))) => (user0, Seq((product0, rating))) }
                                      .reduceByKey(_ ++ _).map(s => (s._1, s._2.map(s0 => (s0._1, s0._2.toString)).toMap))

    /**
    //------------------------------------------------------------
    val userIndexAndProd = userBlockMatrix.multiply(productBlockMatrix)
      .toCoordinateMatrix().entries
      .map{ case MatrixEntry(user, product, rating) => (user,(product, rating))}
      .aggregateByKey(initValue)(aggregate, combine)

    val temp3RDD = userIndexAndProd.map{ case (user, products) =>
      for ((productIndex, rating) <- products) yield (productIndex, (user, rating))}.flatMap(s => s)

    val replaceProductIndex = productIndex.map(s=> (s._2, s._1)).join(temp3RDD)
      .map{ case (productIndex, (product, (user, rating))) => (user, (product, rating))}

    val replaceUserIndex = cookieIndex.map(s=>(s._2, s._1)).join(replaceProductIndex)
      .map{ case (userIndex, (user, (product, rating))) => (user, Seq((product, rating)))}
    .reduceByKey(_ ++ _).map( s => ("rcmd_guess_" + s._1, s._2.sortWith(_._2 > _._2).map(_._1).mkString("#")))
    //---------------------------------------------------------------

    // ==========================
      */

    val count = replaceItemIndex.sparkContext.accumulator(0)
    //    saveToRedis(userItemTupleString, count)
    saveMapToRedis(replaceItemIndex, count)
    Message.addMessage(s"insert into redis :  ${count.value}")




    val userProduct = userBlockMatrix.multiply(productBlockMatrix)
                                      .toCoordinateMatrix().entries
                                      .map{ case MatrixEntry(user0, product0, rating) => (user0,(product0, rating))}
                                      .aggregateByKey(initValue)(aggregate, combine)
                                      .map(s => (s._1, s._2.mkString("#")))
    userProduct
  }

  /** 采用笛卡尔积方式计算 user-item 矩阵*/

  def cartesian(cookieIndex: RDD[(String, Long)], productIndex: RDD[(String, Long)], model: MatrixFactorizationModel): Unit = {
//    val cookiePath = ConfigurationBL.get("als.cookie.path")
//    val productPath = ConfigurationBL.get("als.product.path")
//    val sc = SparkFactory.getSparkContext
//    sc.textFile(cookiePath).map(s => { val  a = s.split("\t"); (a(1).toInt, a(0))})
//    sc.textFile(productPath).map(s => { val  a = s.split("\t"); (a(1).toInt, a(0))})


    val indexUser = cookieIndex.map(s => (s._2.toInt, s._1))

    val user = model.userFeatures.join(indexUser).map{ case (index, (ratings, cookie)) => (cookie, ratings)}
    val indexProd = productIndex.map(s => (s._2.toInt, s._1))
    val item = model.productFeatures.join(indexProd).map { case  (index, (ratings, itemNo)) => (itemNo, ratings)}
    val takeNum = ConfigurationBL.get("als.take.num", "30").toInt
    val initValue = new Array[(String, Double)](takeNum).map(s =>("", -1.0))
    val aggregate = (init: Array[(String, Double)], toBeAdd: (String, Double)) => insertSortCartesian(init, toBeAdd)
    val combine = (op1: Array[(String, Double)], op2: Array[(String, Double)]) => { for (v <- op2) insertSortCartesian(op1, v); op1 }
    val userItem= user.cartesian(item)
    userItem.persist(StorageLevel.MEMORY_AND_DISK)
//    userItem.sparkContext.setCheckpointDir("/user/tmp/spark/checkpoint")
//    userItem.checkpoint() // user-item 太大了，重新计算太过于麻烦，不知道这样能不能提高计算速度
    val userItemRating = userItem.map { case ((user0, userRating), (item0, itemRating)) =>
                                    (user0, (item0, calculate(userRating, itemRating))) }
    val pickUserItem = userItemRating.aggregateByKey(initValue)(aggregate, combine)
    val result = pickUserItem.map(s => (s._1, s._2.map(s0 => (s0._1, s0._2.toString)).toMap))

    val count = SparkFactory.getSparkContext.accumulator(0)
    saveMapToRedis(result, count)
    Message.addMessage(s"als model count:\t$count")

  }



  /** 将所有用户聚集到driver，然后一个一个推荐。*/
  def recommend(cookieIndex: RDD[(String, Long)], productIndex: RDD[(String, Long)], model: MatrixFactorizationModel): Unit = {
    val num = ConfigurationBL.get("als.take.num", "30").toInt
    // 商品编号的 map
    val product = productIndex.map(s => (s._2.toInt, s._1)).collectAsMap()
    val indexUser = cookieIndex.map(s => (s._2.toInt, s._1))

//    model.userFeatures.cache()
//    model.productFeatures.cache()
    // 将所有用户 collect 到 driver 节点
    val users = model.userFeatures.map(_._1).collect()

    val result = users.map { case user => (user, model.recommendProducts(user, num)) }
      .map { case (user, products) => (user, products.map(p => (product(p.product), p.rating.toString))) }
    val resultRDD = SparkFactory.getSparkContext.parallelize(result).join(indexUser).map { case (index, (p, cookie)) => (cookie, p.toMap)}
    val count = SparkFactory.getSparkContext.accumulator(0)
    saveMapToRedis(resultRDD, count)
    Message.addMessage(s"als model count:\t$count")

  }




  def getMemory(memory: String): Int = {
    if ( memory.endsWith("g") ) {
      memory.substring(0, memory.indexOf("g")).toInt
    } else if (memory.endsWith("G")) {
      memory.substring(0, memory.indexOf("G")).toInt
    } else if (memory.endsWith("m")) {
      memory.substring(0, memory.indexOf("m")).toInt
    } else if (memory.endsWith("M")) {
      memory.substring(0, memory.indexOf("M")).toInt
    } else {
      throw new Exception("error memory munber!")
    }
  }

  def calculate(array1: Array[Double], array2: Array[Double]): Double ={
    require(array1.length == array2.length, "user array must be equal item array!")
    var sum = 0.0
    for (i <- array1.indices) sum += array1(i) * array2(i)
    sum
  }

  /** 插入排序法 1 */
  def insertSort(array: Array[(Long, Double)], v: (Long, Double)): Array[(Long, Double)] = {
    var index = -1
    for (i <- array.length - 1 to (0, -1) if array(i)._2 < v._2)  index = i
    if (index == -1) array
    else {
      for (i <- array.length - 1 until (index, -1)) {
        array(i) = array(i -1)
      }
      array(index) = v
      array
    }
  }

  /** 插入排序法 2 */
  def insertSort2(array: Array[(Long, Double)], v: (Long, Double)): Array[(Long, Double)] ={
    val size = array.length - 1
    var i = size
    while ( i >= 0 && v._2 > array(i)._2 ) {
      if ( i != size ) {
        array(i + 1) = array(i)
        array(i) = v
      } else {
        array(i) = v
      }
      i -= 1
    }
    array
  }

  /** 插入排序法 2 */
  def insertSortCartesian(array: Array[(String, Double)], v: (String, Double)): Array[(String, Double)] = {
    val size = array.length - 1
    var i = size
    while ( i >= 0 && v._2 > array(i)._2 ) {
      if ( i != size ) {
        array(i + 1) = array(i)
        array(i) = v
      } else {
        array(i) = v
      }
      i -= 1
    }
    array
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
    val limit = ConfigurationBL.get("als.day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
  }

  def saveMapToRedis(rdd: RDD[(String, Map[String, String])], accumulator: Accumulator[Int]): Unit = {
    val alsPrefix = ConfigurationBL.get("als.key.prefix")
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        val key = alsPrefix + s._1
        if (jedis.exists(key)) jedis.del(key)
        jedis.hmset(key, s._2)

        accumulator += 1
      })
      jedis.close()
    })
  }


  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        jedis.set(s._1, s._2)
        accumulator += 1
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
