package com.bl.bigdata.redis

/**
 * Created by MK33 on 2016/5/20.
 */
class RedisSuccessMessage(val host: String,
                          val port: Int = 6379,
                          val timeout: Int = 1000) extends RedisMessage {
  var status = true
  var toWrite = 0L
  var success = 0L

  def set(in: Long, success: Long) {
    this.toWrite = in
    this.success = success
  }

  override def toString: String = {
    s"""redis success message:
      |   host: $host
      |   port: $port
      |   timeout: $timeout
      |   in: $toWrite
      |   success: $success
    """.stripMargin
  }


}
