package com.bl.bigdata.machinelearning

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/5/16.
 */
class LRGender {

  def train(): Unit = {
    val sc = SparkFactory.getSparkContext("gender_train")
    val hiveContext = new HiveContext(sc)
    val sql = "SELECT member_id,gender,level2_id,goods_sid,event_date FROM recommendation.train_data_lev1"
    val userCategoryRdd  = hiveContext.sql(sql).rdd.map(row=>((row.getString(0),row.getString(1)),(row.getLong(2).toInt)))
    val maxCateId = userCategoryRdd.values.max()
    val userCatesRdd = userCategoryRdd.mapValues(x=>List(x)).reduceByKey(_ ++ _).filter(s=>(s._2.length<20)&&(s._2.length>3))
    val uccRdd = userCatesRdd.flatMapValues(x=>
      {
        val total = x.length
        val dx = x.distinct
        dx.map(y=>{
          val cnt = x.filter(_ .equals(y)).length
          (y,cnt.toDouble/total)
        })
      }).mapValues(x=>Set(x)).reduceByKey(_ ++ _).filter(( s =>s._1._2.equals("1") || s._1._2.equals("0")))
    uccRdd.foreach(println)
    val catVecRdd = uccRdd.map(x=>
     {
       val seq= x._2.toSeq
       val vec = Vectors.sparse(maxCateId,seq)
       LabeledPoint(x._1._2.toInt,vec)
     })
    catVecRdd.foreach(println)

//    val splits = catVecRdd.randomSplit(Array(0.6, 0.4), seed = 11L)
//    val training = splits(0).cache()
//    val test = splits(1)
//
//    val model = new LogisticRegressionWithLBFGS()
//      .setNumClasses(2)
//      .run(training)
//
//    model.clearThreshold
//
//    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
//      val prediction = model.predict(features)
//      (prediction, label)
//    }
//    println("_______________________________________  ")
//    println("_______________________________________  ")
//    println("_______________________________________  ")
//    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
//    val precision = metrics.precisionByThreshold
//    precision.foreach {
//      case (t, p) =>
//        println(s"Threshold: $t, Precision: $p")
//    }


     sc.stop()
  }




}

object LRGender {
  def main(args: Array[String]) {
    val lrg = new LRGender
   lrg.train()
  }

}
