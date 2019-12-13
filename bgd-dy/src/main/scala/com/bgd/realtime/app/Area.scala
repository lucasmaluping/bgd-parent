package com.bgd.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.bgd.common.constant.MoveConstant
import com.bgd.realtime.bean.Startuplog
import com.bgd.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
  * @author dengyu
  * @data 2019/12/10 - 17:19
  * 北京地区实时统计上线人数及操作系统的比例（登陆过就算上线，同一个id去重）
  *      os area  mid
  *      算出来实时的mid总数
  */

object Area {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local")

    val ssc = new StreamingContext(conf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(MoveConstant.KAFKA_TOPIC_STARTUP,ssc)
    ssc.sparkContext.setCheckpointDir("cp")

    val value: DStream[Startuplog] = inputDstream.map { rdd =>
      val str: String = rdd.value()
      val startuplog: Startuplog = JSON.parseObject(str, classOf[Startuplog])

      val datetimeString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startuplog.ts))

      startuplog.logDate = datetimeString.split(" ")(0)
      startuplog.logHour = datetimeString.split(" ")(1)
      startuplog
    }

    var i :Long = 0;
    var k :Long = 0;

    val end: DStream[(String, String, String)] = value.transform {
      rdd =>
        val jedis: Jedis = RedisUtil.getJedisClient //driver //按周期执行
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

        val key = "dau:" + dateStr
        val dauMidSet: util.Set[String] = jedis.smembers(key)
        val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
        //println("过滤前：" + rdd.count())
        jedis.close()
        val filteredRDD: RDD[Startuplog] = rdd.filter { startuplog => //executor
          val dauMidSet: util.Set[String] = dauMidBC.value
          !dauMidSet.contains(startuplog.mid)
        }
        //println("过滤后：" + filteredRDD.count())
        val mapp: RDD[(String, String, String)] = filteredRDD.map {
          a => (a.area, a.os, a.mid)
        }
        val area: RDD[(String, String, String)] = mapp.filter {
          f =>
            f._1.contains("beijing")
        }
        val os: RDD[(String, String, String)] = mapp.filter {
          t =>
            t._1.contains("beijing") && t._2.contains("ios")
        }
        //o为ios数量
        val l: Long = os.count()
        k += l
        val d: Double = k.toDouble/i.toDouble
        //println(area.count())
        var j :Long = area.count()
        //i为总数
        i += j
        println("++++++++++++++++++++++++++_________上线人数____________++++++++++++++++++++"+i)
        println("++++++++++++++++++++++++++============ios所占比例=================++++++++++++++++++++" + d)
        area
    }
    end.saveAsTextFiles("D:/BaiduNetdiskDownload/")

    ssc.start()
    ssc.awaitTermination()

  }

}
