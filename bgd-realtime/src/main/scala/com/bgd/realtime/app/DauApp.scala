package com.bgd.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.bgd.common.constant.MoveConstant
import com.bgd.common.utils.MyEsUtil
import com.bgd.realtime.bean.Startuplog
import com.bgd.realtime.util.MyKafkaUtil
import com.bgt.realtime.utils.MyRedisUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/** *
  *
  * @author :star
  * @time :2019.12.7
  * @message:
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    //构建一个SparkStreaming的环境
    val sparkConf: SparkConf = new SparkConf().setAppName("DateDispose").setMaster("local[*]")
    //5秒收集一次数据
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //用封装好的kafkaUtil类来获取kafka的实例
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MoveConstant.KAFKA_TOPIC_STARTUP, ssc)
//    inputDstream.count().print()

    //    inputDstream.foreachRDD{rdd=>
    //      println(rdd.map(_.value()).collect().mkString("\n"))
    //    }



    // 转换处理
    val startuplogStream: DStream[Startuplog] = inputDstream.map { record =>
      //kafka中数据是<k,v>，我们只需要v
      val jsonStr: String = record.value()
      //将string转换成json对象
      val startuplog: Startuplog = JSON.parseObject(jsonStr, classOf[Startuplog])
      //获取时间戳转换格式
      val date = new Date(startuplog.ts)
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
      //根据" " 切后就是 （yyyy-MM-dd，HH:mm）数组
      val dateArr: Array[String] = dateStr.split(" ")
      startuplog.logDate = dateArr(0)//yyyy-MM-dd
      startuplog.logHour = dateArr(1).split(":")(0)//HH
      startuplog.logHourMinute = dateArr(1)//HH:mm

      startuplog
    }
    // 利用redis进行去重过滤
    //考虑到spark执行在driver和executor中，所以我们采取transform算子
    val filteredDstream: DStream[Startuplog] = startuplogStream.transform { rdd =>
      println("过滤前：" + rdd.count())
      //driver  周期性执行
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
      //MyRedisUtil用的是java代码构造的
      val jedis: Jedis = MyRedisUtil.getJedisClient
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
        !dauSet.contains(startuplog.mid)
      }
      println("过滤后：" + filteredRDD.count())
      filteredRDD

    }

    //去重思路;把相同的mid的数据分成一组 ，每组取第一个
    val groupbyMidDstream: DStream[(String, Iterable[Startuplog])] = filteredDstream.map(startuplog => (startuplog.mid, startuplog)).groupByKey()
    val distinctDstream: DStream[Startuplog] = groupbyMidDstream.flatMap { case (mid, startulogItr) =>
      startulogItr.take(1)
    }


    distinctDstream.count().print()

//    // 保存到redis中
//    distinctDstream.foreachRDD { rdd =>
//      //driver
//      // redis  type set
//      // key  dau:2019-06-03    value : mids
//      rdd.foreachPartition { startuplogItr =>
//        //executor
//        val jedis: Jedis = MyRedisUtil.getJedisClient
//        val list: List[Startuplog] = startuplogItr.toList
//        for (startuplog <- list) {
//          val key = "dau:" + startuplog.logDate
//          val value = startuplog.mid
//          jedis.sadd(key, value)
//          //println(startuplog) //往es中保存
//        }
//        //将最终处理的数据保存到ES中
//        MyEsUtil.indexBulk(MoveConstant.ES_INDEX_DAU, list)
//        jedis.close()
//      }
//
//      //      rdd.foreach { startuplog =>   //executor
//      //        val jedis: Jedis = RedisUtil.getJedisClient
//      //        val key="dau:"+startuplog.logDate
//      //        val value=startuplog.mid
//      //        jedis.sadd(key,value)
//      //        jedis.close()
//      //      }
//
//
//    }


    ssc.start()

    ssc.awaitTermination()


  }

}
