package com.bl.bigdata.tfidf

import breeze.linalg.{norm, SparseVector => BSV}
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Accumulator, HashPartitioner}
import org.apache.spark.mllib.linalg.{SparseVector, _}
import org.apache.spark.rdd.RDD

import scala.collection.mutable


/**
  * 计算同一类别中两两商品的相似度。
  * 计算方法：
  * 对于同一类别的商品，搜集它们的属性，然后这个类别的商品，根据这些属性进行向量化，
  * 得到TF-IDF，计算余弦距离。例如对于某个类别 C 商品，它的属性集为 （A,B,C,D,E)。
  * 因为一个商品的属性要么出现要么不出现，故向量中的每一项值为 0 或者 1。
  * 如 a 商品的属性为 A、B、C，则 a 商品向量化以后为 va = [1,1,1,0,0]
  * b 商品的属性为 C、D、E，则 b 商品向量化以后为 vb = [0,0,1,1,1]
  * 所以商品向量化以后的含义是，如果某一项为1，则代表有这个属性，0则代表不存在这个属性。
  *
  * Created by MK33 on 2016/3/9.
  */
class GoodsSimilarityInCate extends Tool with Serializable {

  /** TF-IDF 的维度大小*/
  val featuresNum = 1 << 16

  var optionMap: Map[String, String] = _

  override def run(args: Array[String]): Unit = {
   optionMap = try {
     GoodsSimInCateConf.parse(args)
   } catch {
     case e: Throwable =>
       logger.error("parse command line error: " + e)
       GoodsSimInCateConf.printlnHelp
       return
   }

    logger.info("同类商品属性的相似性开始计算.........")
    Message.addMessage("\n同类商品属性的相似性：\n")

    val in = optionMap(GoodsSimInCateConf.in)
    val output = optionMap(GoodsSimInCateConf.out)
    val redis = output.contains("redis")
    val hbase = output.contains("hbase")

    val table = ""
    val columnFamily = ""
    val columnName = ""

    val sc = SparkFactory.getSparkContext("同类商品属性相似度")
    val sql =
      """
        |select p.sid, p.mdm_goods_sid, p.category_id, p.brand_sid, p.sale_price, p.value_sid, g.store_sid
        |from recommendation.product_properties_raw_data p inner join recommendation.goods_avaialbe_for_sale_channel g on g.sid = p.sid
        |where p.category_id is not null
        |
      """.stripMargin

    val sqlName = optionMap(GoodsSimInCateConf.sqlName)

    val hiveContext = SparkFactory.getHiveContext

    val goodsSql = SqlFactory.getSql(sqlName)
    val parallel = hiveContext.sparkContext.defaultParallelism

    val rawDF = hiveContext.sql(goodsSql)
    val rawRDD = rawDF.rdd.map(r => (r.getString(0), r.getString(1), if (r.isNullAt(2)) null else r.getLong(2), r.getString(3),
      if (r.isNullAt(4)) null else r.getDouble(4), r.getString(5), r.getString(6))).filter(s => s._3 != null && s._5 != null).
      map { case (goodsID, itemNo, category, band, price, attribute, storeId) =>
        (goodsID, itemNo, category + "_" + storeId, band, attribute, (price.toString.toDouble * 100).toInt / 100.0) }.
      distinct().repartition(parallel)
    rawRDD.cache()



//    val a = rawRDD.map(s=> (s._3, s._1)).distinct().map(_._1).countByValue().toSeq.sortWith(_._2 > _._2)

//    val rawRDD = DataBaseUtil.getData(in, sqlName).filter(s => s(0) != "null" && s(1) != "null" && s(2) != "null" && s(3) != "null"&& s(4) != "null" && s(5) != "null")
//                                  .map { case Array(goodsID, itemNo, category, band, price, attribute, storeId) =>
//                                         (goodsID, itemNo, category + "_" + storeId, band, attribute, price)
//                                  }.distinct()
    // 计算每个类别的商品价格分布，分为 5 份，假设每个类别的商品价格数大于 5 个。
    val categoryRDD = rawRDD.map { case (goodsID, itemNo, category, band, attribute, price) => (category, price) }
                            .map { case (category, price) => (category, mutable.Seq(price.toDouble)) }
                            .reduceByKey(_ ++ _)
                            .map { case (category, priceSeq) =>
                              val sortedSeq = priceSeq.distinct.sorted
                              val size = sortedSeq.size
                              // 0%, 20%, 40%, 60%, 80%
                              (category, List(0.0, sortedSeq(size * 2 / 10), sortedSeq(size * 4 / 10),
                                sortedSeq(size * 6 / 10), sortedSeq(size * 8 / 10)))
                            }

    val tf = rawRDD.map { case (goodsID, itemNo, category, brand, attribute, price) =>
                          ((goodsID, itemNo, category, brand, price), mutable.Seq(attribute)) }
                    // 搜集某个商品的属性
                    .reduceByKey(_ ++ _)
                    .map { case ((goodsID, itemNo, category, brand, price), attributes) =>
                      (category, (itemNo, goodsID, brand, attributes, price))}
                    .join(categoryRDD)
                    .map { case (category, ((itemNo, goodsID, brand, attributes, price), priceArray)) =>
                      // 计算商品属性的 TF
                      val attrVector = calculatorTF(attributes, price.toString, priceArray)
                      (category, (goodsID, itemNo, brand, attrVector))
                    }

    //calculator IDF
    val idf: RDD[(String, Vector)] = tf.aggregateByKey(new DocumentFrequencyAggregator())(seqOp = (df, v) => df.add(v._4),
      combOp = (df1, df2) => {
        df1.merge(df2)
      }).map { case (category, df) => (category, df.idf()) }
    // calculator tf-idf
    val tfIDF = tf.join(idf).map { case (category, ((no, itemNo, brand, attrVector), idf2)) =>
      (category, (no, IDFModel.transform(idf2, attrVector)))
    }

    tfIDF.partitionBy(new org.apache.spark.HashPartitioner(parallel))
    tfIDF.cache()
//    tfIDF.repartition(20)
//    val t = tfIDF.map(s => (s._1, 1)).reduceByKey(_ + _).sortBy(_._2, false).collect
//    t.maxBy(_._2)
//    val tt = t.map(s => ((s._1.## % 20) + (if (s._1.## % 20 < 0) 20 else 0), s._2)).groupBy(_._1).map(s => (s._1, s._2.map(_._2).sum))


    // calculator simplicity in category
    val similarity = tfIDF.join(tfIDF).filter(s => s._2._1._1 != s._2._2._1).map { case (category, (tfidf1, tfidf2)) =>
      val id1 = tfidf1._1
      val id2 = tfidf2._1
      val cosSim = if (id1.equals(id2)) 0.0
      else {
        val v1 = tfidf1._2.asInstanceOf[SparseVector]
        val v2 = tfidf2._2.asInstanceOf[SparseVector]
        var sum = 0.0
        for (v <- v1.indices) {
          sum += v1(v) * v2(v)
        }
        val s1 = v1.values.map(s => s * s).sum
        val s2 = v2.values.map(s => s * s).sum

//        println(sum / Math.sqrt(s1 * s2))
        sum / Math.sqrt(s1 * s2)
//        val sv1 = tfidf1._2.asInstanceOf[SV]
//        val bsv1 = new BSV[Double](sv1.indices, sv1.values, sv1.size)
//        val sv2 = tfidf2._2.asInstanceOf[SV]
//        val bsv2 = new BSV[Double](sv2.indices, sv2.values, sv2.size)
//        // calculator the cosin simplicity
//        bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
      }
      (id1, (id2, cosSim))
    }

    val count = 20
//    val similarityGoods = similarity.map { case (id1, (id2, cosSim)) => (id1, mutable.Seq((id2, cosSim))) }
//      .reduceByKey((s1, s2) => s1 ++ s2)
//      .map { case (id1, seq) => ("rcmd_sim_" + id1, seq.sortWith((seq1, seq2) => seq1._2 > seq2._2).take(20).map(_._1).mkString("#"))}
//

    val similarityGoods = similarity.aggregateByKey(new Array[(String, Double)](count))(GoodsSimilarityInCate.addToItem, GoodsSimilarityInCate.mergeArray).
      map { case (goods, simiarGoods) => ("rcmd_sim_" + goods, simiarGoods.filter(_ != null).map(_._1).mkString("#"))}


    if (redis) {
      val accumulator = sc.accumulator(0)
      val redisType = if (output.contains(RedisClient.cluster)) RedisClient.cluster else RedisClient.standalone
//      saveToRedis(similarity, accumulator)
      RedisClient.sparkKVToRedis(similarityGoods, accumulator, redisType)
      Message.addMessage(s"\t插入 redis rcmd_sim_* :  $accumulator\n ")
    }

    if (hbase) {
      val hBaseConf = HBaseConfiguration.create()
      hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, table)
      val job = Job.getInstance(hBaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[Put]])

      val columnFamilyBytes = Bytes.toBytes(columnFamily)
      val columnNameBytes = Bytes.toBytes(columnName)
      similarityGoods.map { case (id, seqString) =>
        val put = new Put(Bytes.toBytes(id))
        (new ImmutableBytesWritable(id.getBytes),
          put.addColumn(columnFamilyBytes, columnNameBytes, Bytes.toBytes(seqString)))
      }
        .saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
    rawRDD.unpersist()
    logger.info("同类商品属性的相似性计算结束。")
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



  /**
    * 计算商品的属性和价格的 TF
    * @param array 属性集
    * @param price 价格
    * @param priceArray 价格的等级
    */
  def calculatorTF(array: Traversable[String], price: String, priceArray: List[Double]): Vector = {
    // 计算商品属性的 TF
    val termFrequency = mutable.HashMap.empty[Int, Double]
    if (price.equalsIgnoreCase("NULL"))
      termFrequency(featuresNum) = (priceArray.size + 1) / 2
    else {
      var i = priceArray.size
      while (i > 0 && termFrequency.isEmpty) {
        // 0%-20%: 1, 20%-40%: 2, 40%-60%: 3, 60%-80%: 4, 80%- : 5
        if (price.toDouble >= priceArray(i - 1)) termFrequency(featuresNum) = i
        i -= 1
      }
    }
    array.foreach { attr =>
      val index = indexOf(attr)
      termFrequency.put(index, termFrequency.getOrElse(index, 0.0) + 1.0)
    }
    Vectors.sparse(featuresNum + 1, termFrequency.toSeq)
  }

  def indexOf(term: Any): Int = nonNegativeMod(term.##, featuresNum)

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

}

object GoodsSimilarityInCate extends Serializable {

  def main(args: Array[String]) = {
    execute(args)
  }

  def execute(args: Array[String]) = {
    val goodsSimilarity = new GoodsSimilarityInCate with  ToolRunner
    goodsSimilarity.run(args)
  }

  val addToItem = (array: Array[(String, Double)], item: (String, Double)) => {
    var size = array.length - 1
    if (array(size) == null || array(size)._2 < item._2) {
      array(size) = item
      size -= 1
      while (size >= 0 && (array(size) == null || (array(size)._2 < item._2))) {
        array(size + 1) = array(size)
        array(size) = item
        size -= 1
      }
    }
    array
  }

  val  mergeArray = (a1: Array[(String, Double)], a2: Array[(String, Double)]) => {
    val size = a1.length
    assert(a1.length == a2.length)
    val r = new Array[(String, Double)](size)
    var m1 = 0
    var m2 = 0
    // model m1
    var j = 0
    while ((a1(m1) != null || a2(m2) != null) && j < a1.length) {
      if (a1(m1) == null) {
        r(j) = a2(m2)
        m2 += 1
      } else if (a2(m2) == null) {
        r(j) = a1(m1)
        m1 += 1
      } else if (a1(m1)._2 < a2(m2)._2) {
        r(j) = a2(m2)
        m2 += 1
      } else {
        r(j) = a1(m1)
        m1 += 1
      }
      j += 1
    }

    r
  }

}


object GoodsSimInCateConf {

  val in = "input"
  val out = "out"
  val sqlName = "sql_name"

  val commandLine = new MyCommandLine("GoodsSimilarityInCate")

  commandLine.addOption("i", in, true, "input data source type", "hive")
  commandLine.addOption("o", out, true, "output result", "redis-" + RedisClient.cluster)
  commandLine.addOption("sql", sqlName, true, "sql name in hive.xml", "goods.similarity.in.cate")

  def parse(args: Array[String]): Map[String, String] = commandLine.parser(args)

  def printlnHelp = commandLine.printHelper

}