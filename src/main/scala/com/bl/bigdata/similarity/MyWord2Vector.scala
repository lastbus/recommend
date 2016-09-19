package com.bl.bigdata.similarity


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.Word2Vec

/**
  * Created by MK33 on 2016/7/22.
  */
object MyWord2Vector {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("deep learning"))
    val rawRDD = sc.textFile("D:\\IdeaProjects15\\recommend-mk\\wordxx.txt")

    val trainRDD = rawRDD.map(line => line.split(" ").toSeq)
    trainRDD.cache()
//    println(trainRDD.first().length)

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
    val model = word2Vec.fit(trainRDD)
//    val r = model.transform("201280")
    println(model.findSynonyms("297343", 10).mkString(", "))

  }

}
