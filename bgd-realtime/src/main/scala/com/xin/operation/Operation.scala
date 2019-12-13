package com.xin.operation

import java.lang

import com.alibaba.fastjson.JSON
import com.bgd.common.constant.MoveConstant
import com.bgd.realtime.bean.Startuplog
import com.bgd.realtime.util.MyKafkaUtil
import com.xin.util.JedisPoolUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 *  需求： 北京地区实时统计上线人数及操作系统的比例（登陆过就算上线人数，同一id去重）
 *  组5：{辛佳楠，李东，索杰敏，谢子文，于帅鹏，李万莹，高璐，张征，王艺嘉}
 *
 *  日志样式：{"area":"guangdong","uid":"444","os":"andriod","ch":"tencent","appid":"gmall1205","mid":"mid_212","type":"startup","vs":"1.2.0","ts":1575888069267}
 *  结果样式：北京上线人数：75   andriod人数：57      ios人数：18    -->与redis对应
 */

object Operation {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("operation").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //用封装好的kafkaUtil类来获取kafka的实例
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MoveConstant.KAFKA_TOPIC_STARTUP, ssc)

    //将kafka消费的数据进行过滤只保留北京地区数据，最后数据(mid,os)
    val filters: DStream[(String, String)] = inputDstream.map(dstream => {
      val str: String = dstream.value()
      val startuplog: Startuplog = JSON.parseObject(str, classOf[Startuplog])
      val mid: String = startuplog.mid
      val area: String = startuplog.area
      val os: String = startuplog.os
      (mid, area, os)
    }).filter(_._1 != "").filter(_._2.equals("beijing")).map(d => {
      (d._1, d._3)
    })

    //将数据去重   groupByKey取第一个元素即实现去重
    val groups: DStream[(String, String)] = filters.groupByKey().map(d => {
      val array: Array[String] = d._2.toArray
      val os: String = array(0)
      (d._1, os)
    })

    /*保存数据到Redis 以os为key，mid为value 最后在redis即可统计实时不同系统的人数和总人数*/
    groups.foreachRDD { rdd =>      //foreachRDD遍历DStream里面的RDD
      rdd.foreachPartition { partitionOfRecords =>    //遍历每个分区
        var jedis: Jedis = null
        try {
          jedis = JedisPoolUtil.getConnection     //获得jedis对象
          jedis.auth("123456")
          partitionOfRecords.foreach(x=>{
            val key = x._2
            val value = x._1
            jedis.sadd(key, value)
            x
          })

          val andriodNum: lang.Long = jedis.scard("andriod")
          val iosNum: lang.Long = jedis.scard("ios")
          var num = andriodNum+iosNum
          println("北京上线人数："+num+"   andriod人数："+andriodNum+"      ios人数："+iosNum)
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        } finally {
          if (jedis != null) jedis.close()
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
