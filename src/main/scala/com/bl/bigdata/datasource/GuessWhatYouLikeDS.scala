package com.bl.bigdata.datasource

import org.apache.spark.sql.DataFrame

/**
  * Created by blemall on 4/5/16.
  */
object GuessWhatYouLikeDS extends HiveDataSource {

    override def read(): DataFrame = {
        val hiveContext = connect()
        val data = hiveContext.sql("select * from recommendation.user_behavior_raw_data")
        data
    }
}
