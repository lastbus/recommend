package com.bl.bigdata.tfidf

import com.bl.bigdata.util.{Tool, ToolRunner}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import breeze.linalg.{SparseVector => BSV, norm}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.mllib.linalg.{SparseVector => SV, _}
import org.apache.spark.{SparkConf, SparkContext}
import com.redislabs.provider.redis._

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
object Main extends Tool {

  val featuresNum = 1 << 16

  def main(args: Array[String]): Unit = {
    ToolRunner.run(TFIDFConfiguration, Main, args)
  }


  def indexOf(term: Any): Int = nonNegativeMod(term.##, featuresNum)

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  override def run(args: Array[String]): Unit = {

    if (args.length < 4) {
      println("Please input <input path>,<hBase table name>,<table column Family> and <table column name>.")
      sys.exit(-1)
    }
    val inputPath = args(0)
    val table = args(1)
    val columnFamily = args(2)
    val columnName = args(3)
    val delimiter = "\t"
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.set("redis.host", "10.201.128.216")
    sparkConf.set("redis.timeout", "10000")

    if (!inputPath.startsWith("/")) sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rawRDD = sc.textFile(inputPath)
      .map(line => {
        // 提取字段：商品 id，商品编号，商品种类，商品品牌，商品价格，商品属性
        val attributes = line.split(delimiter)
        // 商品 id，商品编号，商品种类，商品品牌，商品属性，商品价格
        (attributes(0), attributes(1), attributes(2), attributes(3), attributes(5), attributes(4))
      })

    // 计算每个类别的商品价格分布，分为 5 份，假设每个类别的商品价格数大于 5 个。
    val categoryRDD = rawRDD.map { case (goodsID, itemNo, category, band, attribute, price) => (category, price) }
      .filter { case (category, price) => !price.equalsIgnoreCase("NULL") }
      .map { case (category, price) => (category, Seq(price.toDouble)) }
      .reduceByKey(_ ++ _)
      .map { case (category, priceSeq) =>
        val sortedSeq = priceSeq.distinct.sorted
        val size = sortedSeq.size
        // 0%, 20%, 40%, 60%, 80%
        (category, List(0.0, sortedSeq(size * 2 / 10), sortedSeq(size * 4 / 10),
          sortedSeq(size * 6 / 10), sortedSeq(size * 8 / 10)))
      }

    //    categoryRDD.take(50).foreach(println)
    val tf = rawRDD.map { case (goodsID, itemNo, category, brand, attribute, price) =>
      (goodsID, (itemNo, category, brand, Seq(attribute), price))
    }
      // 搜集某个商品的属性
      .reduceByKey((s1, s2) => {(s1._1, s1._2, s1._3, s1._4 ++ s2._4, s1._5)})
      .map { case (goodsID, (itemNo, category, brand, attributes, price)) =>
        (category, (itemNo, goodsID, brand, attributes, price))}
      .join(categoryRDD)
      .map { case (category, ((itemNo, goodsID, brand, attributes, price), priceArray)) =>
        // 计算商品属性的 TF
        val attrVector = calculatorTF(attributes, price, priceArray)
        (category, (goodsID, itemNo, brand, attrVector))
      }

    //calculator IDF
    val idf: RDD[(String, Vector)] = tf.aggregateByKey(new DocumentFrequencyAggregator())(seqOp = (df, v) => df.add(v._4),
      combOp = (df1, df2) => {
        df1.merge(df2)
      })
      .map { case (category, df) => (category, df.idf()) }
    // calculator tf-idf
    val tfIDF = tf.join(idf).map { case (category, ((no, itemNo, brand, attrVector), idf2)) =>
      (category, (no, IDFModel.transform(idf2, attrVector)))
    }

    // calculator simplicity in category
    val similarity = tfIDF.join(tfIDF).map { case (category, (tfidf1, tfidf2)) =>
      val id1 = tfidf1._1
      val id2 = tfidf2._1
      val cosSim = if (id1.equals(id2)) 0.0
      else {
        val sv1 = tfidf1._2.asInstanceOf[SV]
        val bsv1 = new BSV[Double](sv1.indices, sv1.values, sv1.size)
        val sv2 = tfidf2._2.asInstanceOf[SV]
        val bsv2 = new BSV[Double](sv2.indices, sv2.values, sv2.size)
        // calculator the cosin simplicity
        bsv1.dot(bsv2).asInstanceOf[Double] / (norm(bsv1) * norm(bsv2))
      }
      (id1, (id2, cosSim))
    }.filter { case (id1, (id2, cosSim)) => cosSim > 0.0 } // 相似度低的过滤
      .map { case (id1, (id2, cosSim)) => (id1, Seq((id2, cosSim))) }
      .reduceByKey((s1, s2) => s1 ++ s2)
      .map { case (id1, seq) => ("rcmd_sim_" + id1, seq.sortWith((seq1, seq2) => seq1._2 > seq2._2).take(20).map(_._1).mkString("#"))}

//    sc.toRedisKV(similarity)

    similarity.collect{ case (a, b) => a.equals("0")}
    if (inputPath.startsWith("/")) {
      // save to hbase
      val hBaseConf = HBaseConfiguration.create()
      hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      hBaseConf.set("hbase.zookeeper.quorum", "slave14.bl.com,master11.bl.com,slave15.bl.com,slave13.bl.com,master12.bl.com")

      hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, table)
      val job = Job.getInstance(hBaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      job.setOutputFormatClass(classOf[TableOutputFormat[Put]])

      val columnFamilyBytes = Bytes.toBytes(columnFamily)
      val columnNameBytes = Bytes.toBytes(columnName)
      similarity.map { case (id, seqString) =>
        val put = new Put(Bytes.toBytes(id))
        (new ImmutableBytesWritable(id.getBytes),
          put.addColumn(columnFamilyBytes, columnNameBytes, Bytes.toBytes(seqString)))
      }
        .saveAsNewAPIHadoopDataset(job.getConfiguration)
    } else {
      similarity.collect{ case s => s._1.equals("203693")}.foreach(println)
    }
    sc.stop()
  }

  /**
    * 计算商品的属性和价格的 TF
    * @param array 属性集
    * @param price 价格
    * @param priceArray 价格的等级
    */
  def calculatorTF(array: Traversable[String], price: String, priceArray: List[Double]): Vector ={
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

}
