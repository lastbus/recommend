package com.bl.bigdata.datasource

import org.apache.spark.sql.DataFrame

/**
  * Created by blemall on 4/5/16.
  */
trait DataSource {
    def read(): DataFrame
}
