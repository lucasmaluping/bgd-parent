package com.xin.operation

import com.bgd.common.constant.MoveConstant
import com.bgd.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}

object windows {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("23").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //将ConsumerRecord类注册为使用Kyro序列化
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("window", ssc)

    //windows(windowLength, slideInterval)
    // 窗口大小，滑动步长(都是周期整数倍) 即3个周期数据，一次移动一个周期
    val windowed: DStream[ConsumerRecord[String, String]] = inputDstream.window(Seconds(9),Seconds(3))
    windowed.map(t=>t.value()).map((_,1)).reduceByKey(_+_).print()

//    inputDstream.map(t=>t.value()).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
