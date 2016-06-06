package com.bl.bigdata.mahout

import java.io.File

import com.bl.bigdata.util.Logging
import org.apache.mahout.cf.taste.eval.RecommenderBuilder
import org.apache.mahout.cf.taste.impl.eval.{AverageAbsoluteDifferenceRecommenderEvaluator, GenericRecommenderIRStatsEvaluator}
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel
import org.apache.mahout.cf.taste.impl.neighborhood.{NearestNUserNeighborhood, ThresholdUserNeighborhood}
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity
import org.apache.mahout.cf.taste.model.DataModel
import org.apache.mahout.cf.taste.recommender.Recommender

/**
 * Created by MK33 on 2016/6/2.
 */
object UserBasedRecommend extends Logging
{

  def main(args: Array[String]): Unit =
  {
    logger.debug("========  UserBasedRecommend begin to execute  =======")


    val model = new FileDataModel(new File("dataset.csv"))
    val evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator
    val builder = new MyRecommenderBuilder
    val result = evaluator.evaluate(builder, null, model, 0.9, 0.1)
    logger.debug(result)


    val statsEvaluator = new GenericRecommenderIRStatsEvaluator

    val recommenderBuilder = new RecommenderBuilder() {
      @Override def buildRecommender(model: DataModel): Recommender = {
        val similarity = new PearsonCorrelationSimilarity(model)
        val neighborhood = new NearestNUserNeighborhood(4, similarity, model)
        new GenericUserBasedRecommender(model, neighborhood, similarity)
      }
    }

    val stats = statsEvaluator.evaluate(recommenderBuilder, null, model, null, 4,
      GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0)

    logger.debug(stats.getPrecision)
    logger.debug(stats.getRecall)
    logger.debug("========  UserBasedRecommend ends  =======")


  }

  class MyRecommenderBuilder extends RecommenderBuilder
  {

    override def buildRecommender(dataModel: DataModel): Recommender =
    {
      val similarity = new PearsonCorrelationSimilarity(dataModel)
      val neighborhood = new ThresholdUserNeighborhood(0.1, similarity, dataModel)
      new GenericUserBasedRecommender(dataModel, neighborhood, similarity)
    }

  }




}

