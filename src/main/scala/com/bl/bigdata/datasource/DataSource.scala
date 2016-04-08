package com.bl.bigdata.datasource

import org.apache.spark.sql.DataFrame

/**
  * Created by blemall on 4/5/16.
  */
trait DataSource {
    /**
      * need to overwrite this method when pull data from hive
      * @return
      */
    def read(): DataFrame
}
