package com.xin.operation

import java.io.{File, PrintWriter}
import java.lang

import com.alibaba.fastjson.JSON
import com.bgd.common.constant.MoveConstant
import com.bgd.realtime.bean.Startuplog
import com.bgd.realtime.util.MyKafkaUtil
import com.xin.util.ProducerUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class Operation2{}

object Operation2 extends Serializable {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("operation").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MoveConstant.KAFKA_TOPIC_STARTUP, ssc)

    val filters: DStream[(String, Long)] = inputDstream.map(dstream => {
      val str: String = dstream.value()
      val startuplog: Startuplog = JSON.parseObject(str, classOf[Startuplog])
      val mid: String = startuplog.mid
      val ts: Long = startuplog.ts
      (mid, ts)
    })

    filters.foreachRDD(rdd=>{
      rdd.foreach(x=>{
        val mid: String =x._1
        val ts: Long = x._2
        ProducerUtil.send(mid+","+ts)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
