package com.bl.bigdata.streaming.mykafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Iterator;
import java.util.Properties;

/**
 * maven dependency:
 *  <dependency>
 *      <groupId>org.apache.kafka</groupId>
 *      <artifactId>kafka-clients</artifactId>
 *      <version>0.9.0.1</version>
 *   </dependency>
 *
 * Created by MK33 on 2016/8/4.
 */
public class ConsumerDemoJava
{
    public static void main(String args[])
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.201.129.74:9092");
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer consumer = new KafkaConsumer<String, String>(props);

        String topics = "priceTopic fastGoGoodsTopic activesTopic brandTopic stockTopic pictrueTopic marketonTopic basketTopic productTopic";
        String[] t = topics.split(" ");
        consumer.subscribe(java.util.Arrays.asList(t));
        while (true)
        {
            Iterator<ConsumerRecord> records = consumer.poll(100).iterator();
            while (records.hasNext())
            {
                ConsumerRecord  record = records.next();
                System.out.printf("topic: %s, partition: %s, offset: %s, key: %s, value: %s\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

}
