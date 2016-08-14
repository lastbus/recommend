package com.bl.bigdata.streaming.mykafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
/**
 * Created by MK33 on 2016/7/21.
 */
public class ProducerDemoJava {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.201.129.74:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer producer = new KafkaProducer<>(props);
        for(int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, String>("test00", Integer.toString(i), Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null){
                        System.out.println("topic: " + metadata.topic() + ",  partition: " + metadata.partition() + ", offset: " + metadata.offset());
                    }
                    if (exception != null) {
                        System.out.println(exception);
                    }
                }
            });
            Thread.sleep(1000);
        }
        producer.close();
    }
}
