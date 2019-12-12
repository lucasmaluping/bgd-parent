package com.atguigu.gmall2019.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstant
import com.atguigu.gmall2019.realtime.bean.{Realtimelog, Startuplog}
import com.atguigu.gmall2019.realtime.util.{MyKafkaUtil, RedisUtil}
import com.bgd.common.constant.MoveConstant
import com.bgd.realtime.bean.Startuplog
import com.bgd.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * @author dengyu
  * @data 2019/12/11 - 10:27
  *      实时在线人数（5分钟没有再次收到同一id，同一启动时间的event日志视为下线）
  *      将mid,logtype作为key 1作为value
  *      写一个窗口函数窗口大小为
  *      执行reducebykey当value大于等于2时此人作为在线人数
  *      重新执行！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
  */

object Realtime {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Realtime")

    val ssc = new StreamingContext(conf, Seconds(10))

    val topic: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MoveConstant.KAFKA_TOPIC_EVENT, ssc)

    val startup: DStream[Startuplog] = topic.map { rdd =>
      val data: String = rdd.value()
      val startup: Startuplog = JSON.parseObject(data, classOf[Startuplog])
      startup
    }
    startup.cache()
    val win: DStream[Startuplog] = startup.window(Seconds(50), Seconds(20))

    val one: DStream[(String, Int)] = win.transform { rdd =>
      val realtime: RDD[(String, Int)] = rdd.map { a =>
        (a.mid, 1)
      }
      val value: RDD[(String, Int)] = realtime.reduceByKey(_ + _).filter { rdd =>
        rdd._2 > 1
      }

      val jedis: Jedis = RedisUtil.getJedisClient //driver //按周期执行
    val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS").format(new Date())

      val key = "dau:" + dateStr
      //jedis.append(key,value.count().toString)
      jedis.hset(key,"在线人数",value.count().toString)

      jedis.close()
      value
    }

    one.saveAsTextFiles("D:/BaiduNetdiskDownload/")
    ssc.start()
    ssc.awaitTermination()
  }
}
