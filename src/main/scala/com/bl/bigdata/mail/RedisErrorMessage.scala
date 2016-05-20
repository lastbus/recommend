package com.bl.bigdata.mail

/**
 * Created by MK33 on 2016/5/20.
 */
class RedisErrorMessage(var host: String,
                        var port: Int = 6379,
                        var timeout: Int = 1000) {

  var key: String = _
  var errorMsg: String = _

  def this(host: String, key: String, msg: String) {
    this(host)
    this.key = key
    this.errorMsg = msg
  }

  override def toString(): String = {
    s"""Redis error message:
      |   host: $host
      |   key: $key
      |   errorMsg: $errorMsg
      |   port: $port
      |   timeout: $timeout
    """.stripMargin
  }

}
