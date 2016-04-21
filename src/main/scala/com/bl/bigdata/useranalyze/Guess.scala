package com.bl.bigdata.useranalyze

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.{Item, ReadData}
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulator, SparkContext}

/**
 *
 * 协同过滤算法：
 * 算法实现很简单，目前遇到的问题：计算所有用户的推荐的物品时会出错，
 * 这是因为 user-item 矩阵太大了，内存放不下。
 * 现在处理方法：计算 user-item 矩阵之前计算需要用到的内存，
 * 如果内存不够就不计算，报错然后发邮件。否则继续计算。
 *
 * 下一步是把这个矩阵的中间结果一部分放入磁盘，一部分放入内存。
 *
  * Created by MK33 on 2016/4/12.
  */
class Guess extends Tool {


  override def run(args: Array[String]): Unit = {

    val sc = SparkFactory.getSparkContext("ALSModel")
    trainModel(sc)
    Message.sendMail
    SparkFactory.destroyResource()
  }

  def trainModel(sc: SparkContext): MatrixFactorizationModel = {
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
    val ratingRDD = rawRDD.coalesce(executorCores * executorNum, true).
      map{ case Item(Array(cookie_id, behavior_type, goods_sid, dt)) =>
        (cookie_id, (behavior_type, goods_sid, dt)) }.filter(s =>
      s._2._1 == "1000" || s._2._1 == "2000" ||s._2._1 == "3000" || s._2._1 == "4000" )
    ratingRDD.persist()

    val nowMills = new Date().getTime
    // 得到用户 cookie 的编号，从 0 开始, 序号连续，zipWithUniqueIndex 序号不连续，但是速度快，这个速度慢。
    val cookieIndex = ratingRDD.map(_._1).distinct().zipWithIndex()
    cookieIndex.persist(StorageLevel.DISK_ONLY)
    val cookieNum = cookieIndex.count()
    // 得到商品的编号，从 0 开始， 序号连续，zipWithUniqueIndex 序号不连续，但是速度快，这个速度慢。
    val productIndex = ratingRDD.map(_._2._2).distinct().zipWithIndex()
    productIndex.persist(StorageLevel.DISK_ONLY)
    val productNum = productIndex.count()

    /** 有个问题，从 hive 读入的数据是大量的小文件，有好几千，需要把这些小文件合并成大文件，那么合并的依据是什么？
      * 初步做法是： executor.instances * executor.cores 。
      *
      * */
    val fs = FileSystem.get(new Configuration())
    if ( fs.exists(new Path(cookiePath))) fs.delete(new Path(cookiePath), true)
    cookieIndex.map( s => s._1 + "\t" + s._2 ).saveAsTextFile(cookiePath)
    if ( fs.exists(new Path(productPath)) ) fs.delete(new Path(productPath), true)
    productIndex.map( s => s._1 + "\t" + s._2).saveAsTextFile(productPath)
    Message.addMessage(s"save cookie and product to path:\n\t $cookiePath \t\t$productPath")

    //将用户cookie id 和 产品 id 换成从 0 开始的数字
    /** 这儿用 left join 效果会不会更好一点？ */
    val tempRDD = ratingRDD.join(cookieIndex).map{ case (cookie, ((behavior, goods_sid, dt), userIndex)) => (goods_sid, (behavior, userIndex, dt))}
    val trainRDD = tempRDD.join(productIndex).map { case (goods_sid, ((behavior, userIndex, dt), productIndex)) =>
      ((userIndex.toInt, productIndex.toInt), getRating(behavior, dt, nowMills))
    }.reduceByKey(_ + _).map { case ((userIndex, productIndex), rating) => Rating(userIndex, productIndex, rating)}
    // 将训练数据集保存在内存和磁盘中
    trainRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    Message.addMessage("trainRDD count:\n\t" + trainRDD.count() + "\n")

    val rank = ConfigurationBL.get("als.rank").toInt
    val iterator = ConfigurationBL.get("als.iterator").toInt
    val model = ALS.train(trainRDD, rank, iterator, 0.01)
    // 计算模型的均方差
    val usersProducts = trainRDD.map{ case Rating(user, product, rating) => (user, product) }
    val predict = model.predict(usersProducts).map{ case Rating(user, product, rate) => ((user, product), rate)}
    val ratesAndPredicts = trainRDD.map{ case Rating(user, product, rating) => ((user, product), rating) }
        .join(predict)
    val MSE = ratesAndPredicts.map{case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    Message.addMessage(s"ALS Mean Squared Error =\n\t$MSE \n")

    if (fs.exists(new Path(modelPath))) fs.delete(new Path(modelPath), true)
    fs.close()
    // 保存模型
    model.save(sc, modelPath)
    Message.addMessage(s"save als model to: \n\t$modelPath\n")
    Message.sendMail
    // 计算 user-item 矩阵的大小，如果太大则放弃计算
    // 矩阵的元素类型为 (Long, Long, Double) ==> (8B, 8B, 8B)
    val memoryNeed = 24L * cookieNum * productNum / 1024 / 1024 / 1024
    if (memoryNeed.toDouble / totalMemory > 1) {
      Message.addMessage(s"total memory : $totalMemory, but needs:  $memoryNeed ")
//      return null
    }
    // ============= 以下为计算user-item矩阵，其实可以考虑用笛卡尔积计算而不是用spark的矩阵，以后再说吧。=============
    val initValue = new Array[(Long, Double)](20).map(s =>(-1L,0.0))
    val aggregate = (init: Array[(Long, Double)], toBeAdd: (Long, Double)) => insertSort2(init, toBeAdd)
    val combine = (op1: Array[(Long, Double)], op2: Array[(Long, Double)]) => {for (v <- op2) insertSort2(op1, v); op1}

    val user = model.userFeatures.map{ case (userIndex, rating) =>
      for (r <- 0 until rating.length - 1) yield MatrixEntry(userIndex, r, rating(r)) }.flatMap(s => s)
    val userBlockMatrix = new CoordinateMatrix(user).toBlockMatrix()
    userBlockMatrix.persist(StorageLevel.MEMORY_AND_DISK)  //这个矩阵不大，其实不需要缓存
    val product = model.productFeatures.map{ case (productIndex, rating) =>
      for (p <- 0 until rating.length -1) yield MatrixEntry(productIndex, p, rating(p))}.flatMap(s=>s)
    val productBlockMatrix = new CoordinateMatrix(product).transpose().toBlockMatrix()
    productBlockMatrix.persist(StorageLevel.MEMORY_AND_DISK)  //这个矩阵不大，其实不需要缓存,缓存user-item矩阵就好了

    // 原先计算 user-item 矩阵的结果直接就进行下一步计算，这样spark就看不到 user-item 的引用，而这个矩阵是很大的，
    // 但是spark没有它的引用，所以每一次需要它都会从头开始计算它，所以就很浪费时间了。现在的做法是把这个大大的矩阵
    // 的引用保存起来，而不是不保存引用直接进行下一步计算。。。这spark，把一个复杂的计算 拆分成几次较小的执行，效率
    // 会提高很多。
    val cookieProductMatrix = userBlockMatrix.multiply(productBlockMatrix).toCoordinateMatrix().entries
    val aggregateUserItemMatrix = cookieProductMatrix.map{ case MatrixEntry(user, product, rating) => (user,(product, rating))}
        .aggregateByKey(initValue)(aggregate, combine)
    aggregateUserItemMatrix.persist(StorageLevel.MEMORY_AND_DISK_SER) // 序列化以后缓存在磁盘和内存中

    val productItemTuple = aggregateUserItemMatrix.map{ case (user, products) =>
        for ((productIndex, rating) <- products) yield (productIndex, (user, rating))}.flatMap(s => s)
    val replaceProduct = productIndex.map(s=> (s._2, s._1)).join(productItemTuple)
                        .map{ case (productIndex, (product, (user, rating))) => (user, (product, rating))}
//    replaceProduct.checkpoint() //可以考虑把这个结果 checkpoint 起来
    val replaceItemIndex = cookieIndex.map(s=>(s._2, s._1)).join(replaceProduct)
      .map{ case (userIndex, (user, (product, rating))) => (user, Seq((product, rating)))}
      .reduceByKey(_ ++ _).map( s => ("rcmd_guess_" + s._1, s._2.sortWith(_._2 > _._2).map(_._1)))
    val userItemTupleString = replaceItemIndex.mapValues(p => p.mkString("#"))
//    userItemTupleString.checkpoint() //把它checkpoint 起来，及时导入redis失败，那么也可以重新导入？？？

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

    val count = sc.accumulator(0)
    saveToRedis(userItemTupleString, count)
    Message.addMessage(s"insert into redis :  ${count.value}")
    model
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

  def getAllModel(model: MatrixFactorizationModel): RDD[(Long, String)] = {

    val initValue = new Array[(Long, Double)](20).map(s =>(-1L,0.0))
    val aggregate = (init: Array[(Long, Double)], toBeAdd: (Long, Double)) => insertSort2(init, toBeAdd)
    val combine = (op1: Array[(Long, Double)], op2: Array[(Long, Double)]) => {for (v <- op2) insertSort2(op1, v); op1}

    val user = model.userFeatures.map{ case (userIndex, rating) =>
      for (r <- 0 until rating.length - 1) yield MatrixEntry(userIndex, r, rating(r)) }.flatMap(s => s)
    val userBlockMatrix = new CoordinateMatrix(user).toBlockMatrix()
    userBlockMatrix.persist(StorageLevel.DISK_ONLY_2)

    val product = model.productFeatures.map{ case (productIndex, rating) =>
      for (p <- 0 until rating.length -1) yield MatrixEntry(productIndex, p, rating(p))}.flatMap(s=>s)
    val productBlockMatrix = new CoordinateMatrix(product).transpose().toBlockMatrix()
    productBlockMatrix.persist(StorageLevel.DISK_ONLY_2)


    val userProduct = userBlockMatrix.multiply(productBlockMatrix)
                                      .toCoordinateMatrix().entries
                                      .map{ case MatrixEntry(user, product, rating) => (user,(product, rating))}
                                      .aggregateByKey(initValue)(aggregate, combine)
                                      .map(s => (s._1, s._2.mkString("#")))
    userProduct
  }


  /** 插入排序法 1 */
  def insertSort(array: Array[(Long, Double)], v: (Long, Double)): Array[(Long, Double)] = {
    var index = -1
    for (i <- array.length - 1 to (0, -1) if array(i)._2 < v._2)  index = i
    if (index == -1) return  array
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
