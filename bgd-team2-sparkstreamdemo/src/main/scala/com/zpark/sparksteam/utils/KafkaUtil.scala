package com.zpark.sparksteam.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @Author 组1{侯佳伟，张政，王强，云宇庭，于浩，张瑜}
  * @Date 2019/12/11 9:05
  * @Version 1.0
  */
class KafkaUtil {
  def getKafka(ssc: StreamingContext, topic: String, groupId: String) = {
    //将kafka参数应映射成map
    val kafkaParams = Map[String, Object](

      "bootstrap.servers" -> "hdp-1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "fetch.max.wait.ms" -> Integer.valueOf(500),
      "enable.auto.commit" -> java.lang.Boolean.valueOf(false)
    )
    val topics = Set(topic)
    //通过KafkaUtils创建除data
    val date: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    date
  }
}
