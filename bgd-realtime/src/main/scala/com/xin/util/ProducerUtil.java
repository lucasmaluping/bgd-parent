package com.xin.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerUtil {
    private static KafkaProducer<String, String> producer;

    static {
        Properties props = new Properties();
        //声生产者kafkka地址
        props.put("bootstrap.servers", "hdp-3:9092");
        //key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    //发送消息
    public static void send(String value) {
        //topic   key   推送的消息
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("lixian", null, value);
        producer.send(record);
//        producer.close();
    }

    public static void main(String[] args) {
//        send();
    }
}
