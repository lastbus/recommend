package com.bl.bigdata.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}

/**
 * Created by MK33 on 2016/5/29.
 */
object Test {

  def main(args: Array[String]) {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.201.129.74:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](props)

    val topic = "my-replicated-topic"
    val producerRecord = new ProducerRecord[String, String](topic, "1", "hello world!")
    var i = 0
    while (true)
    {
      kafkaProducer.send(producerRecord)
      i += 1
      if (i > 1000) kafkaProducer.flush()
      Thread.sleep(1000L)
    }
//    kafkaProducer.close()
    sys.addShutdownHook({
      println("exit!")
      kafkaProducer.close()
    })

    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit = {
        println("exit, runtime")
        kafkaProducer.close()
      }
    })

  }
}
