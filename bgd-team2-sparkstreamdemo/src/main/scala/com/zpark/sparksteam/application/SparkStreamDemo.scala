package com.zpark.sparksteam.application

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.zpark.sparksteam.domain.{Arealog, OSlog, Startuplog}
import com.zpark.sparksteam.utils.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @Author 组1{侯佳伟，张政，王强，云宇庭，于浩，张瑜}
  * @Date 2019/12/11 9:00
  * @Version 1.0
  */
object SparkStreamDemo {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamDemo").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(7))

    //从卡夫卡中读取数据
    val kafkaUtil: KafkaUtil = new KafkaUtil
    val kafkaValues: InputDStream[ConsumerRecord[String, String]] = kafkaUtil.getKafka(ssc, "bigdata", "spark")

//    kafkaValues.window(Seconds(14), Seconds(7))
// 转换处理
    /**
      * 数据源：
      * {"area":"tianjin","uid":"121","os":"andriod","ch":"baidu","appid":"Hello-Vs","mid":"mid_162","type":"startup","vs":"1.3.1","ts":1576044095950}
      * {"area":"guangdong","uid":"160","os":"andriod","ch":"tencent","appid":"Hello-Vs","mid":"mid_476","type":"startup","vs":"1.1.1","ts":1576044096189}
      */
    val startuplogStream: DStream[Startuplog] = kafkaValues.map(rdd => {
      val values: String = rdd.value()
      val startuplog: Startuplog = JSON.parseObject(values, classOf[Startuplog])
      startuplog
    }).filter(_.area.equals("beijing")) //Startuplog(37,beijing,andriod)
    startuplogStream.print()
    // 利用redis进行去重过滤
    //考虑到spark执行在driver和executor中，所以我们采取transform算子
    val filteredDstream: DStream[Startuplog] = startuplogStream.transform { rdd =>
      println("过滤前：" + rdd.count())
      //driver  周期性执行
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      //MyRedisUtil用的是java代码构造的
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "dau:" + curdate
      /**
        *  在Redis中，我们可以将Set类型看作为没有排序的字符集合，和List类型一样，
        *  我们也可以在该类型的数据值上执行添加、删除或判断某一元素是否存在等操作。
        *  需要说明的是，这些操作的时间复杂度为O(1)，即常量时间内完成次操作。
        *  Set可包含的最大元素数量是4294967295。
        *  和List类型不同的是，Set集合中不允许出现重复的元素,如果多次添加相同元素，
        *  Set中将仅保留该元素的一份拷贝
        */
      val dauSet: util.Set[String] = jedis.smembers(key)
      //将dauSet设置为广播变量
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      //对数据进行过滤
      val filteredRDD: RDD[Startuplog] = rdd.filter { startuplog =>
        //executor
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startuplog.uid)
      }
      println("过滤后：" + filteredRDD.count())
      filteredRDD
    }

    //去重思路;把相同的uid的数据分成一组 ，每组取第一个
    val groupbyMidDstream: DStream[(String, Iterable[Startuplog])] = filteredDstream.map(startuplog => (startuplog.uid, startuplog)).groupByKey()
    val distinctDstream: DStream[Startuplog] = groupbyMidDstream.flatMap { case (mid, startulogItr) =>
      startulogItr.take(1)
    }


    //统计北京地区的实时上线人数
    val areaValues: DStream[Arealog] = distinctDstream.map(arr => {
      (arr.area, 1)
    }).reduceByKey(_ + _).map(arr => {
      Arealog(arr._1, arr._2)
    })

    //将北京地区实时上线人数保存到Redis中
    areaValues.foreachRDD { rdd =>

          rdd.foreachPartition { arealog =>
            //executor
            val jedis: Jedis = RedisUtil.getJedisClient
            val list: List[Arealog] = arealog.toList
            for (arealog <- list) {
              val key = "dau:" + arealog.area
              val value = arealog.sum.toString
              jedis.sadd(key, value)
            }
            jedis.close()
          }
        }

      //统计北京地区上线用户使用的操作系统
    val osValues: DStream[OSlog] = distinctDstream.map(arr => {
      (arr.os, 1)
    }).reduceByKey(_ + _).map(arr => {
      OSlog(arr._1, arr._2)
    })
    //将北京地区上线用户使用的操作系统的个数保存到Redis中
    osValues.foreachRDD { rdd =>

      rdd.foreachPartition { oslog =>
        //executor
        val jedis: Jedis = RedisUtil.getJedisClient
        val list: List[OSlog] = oslog.toList
        for (oslog <- list) {
          val key = "dau:" + oslog.os
          val value = oslog.sum.toString
          jedis.sadd(key, value)
        }
        jedis.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
