package com.bl.bigdata.util

import org.apache.spark.SparkConf

/**
  * Created by blemall on 4/5/16.
  */
class RedisBase {
    val host = PropertyUtil.get("redis.host")
    val port = PropertyUtil.get("redis.port")
    val timeout = PropertyUtil.get("redis.timeout")

    def connect(sparkConf: SparkConf): Unit ={
        sparkConf.set("redis.host", host)
        sparkConf.set("redis.port", port)
        sparkConf.set("redis.timeout", timeout)
    }
}
