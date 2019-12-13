package com.bgd.realtime.app

import com.bgd.common.constant.MoveConstant
import com.bgd.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * @Author Lucas
  * @Date 2019/12/11 16:15
  * @Version 1.0
  */
object TestWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("TestWindow").setMaster("local[*]")
    //5秒收集一次数据
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //用封装好的kafkaUtil类来获取kafka的实例
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MoveConstant.KAFKA_TOPIC_Window, ssc)
    inputDstream.print()

//    val reduce: DStream[(ConsumerRecord[String, String], Int)] = inputDstream.map(x => (x,1)).reduceByKey(_+_)
//
//    reduce.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
