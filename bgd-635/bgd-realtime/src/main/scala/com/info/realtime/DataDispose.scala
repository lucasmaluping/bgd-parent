package com.info.realtime

import com.alibaba.fastjson.JSON
import com.info.bean.Data
import com.info.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
  * @Author :star
  * @Date :2019/12/10 11:09
  * @Version :1.0
  */
object DataDispose {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("DataDispose")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("D:\\bigdata\\checkpoint")

    //从kafka中获取数据
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("bigdata_635", ssc)

    ssc.start()
    ssc.awaitTermination()
  }

  private def addressCount(kafkaData: InputDStream[ConsumerRecord[String, String]]) = {
    //TODO 5.那个地区的人数最多
    val mapDStream: DStream[String] = kafkaData.map {
      kdata =>
        val kafkaValue: String = kdata.value()
        val jsonData: Data = JSON.parseObject(kafkaValue, classOf[Data])
        jsonData.address
    }
    val valueDStream: DStream[(String, Int)] = mapDStream.map(address => (address, 1))
    val reduceByKeyDStream: DStream[(String, Int)] = valueDStream.reduceByKey(_ + _)
    val groupByKeyDStream: DStream[(String, Iterable[Int])] = reduceByKeyDStream.groupByKey()
    groupByKeyDStream.print()
  }

  private def phoneCount(kafkaData: InputDStream[ConsumerRecord[String, String]]) = {
    //TODO 4.135开头的手机号有那些
    val mapDstream: DStream[String] = kafkaData.map {
      kdata =>
        val kafkaValue: String = kdata.value()
        val jsonData: Data = JSON.parseObject(kafkaValue, classOf[Data])
        jsonData.phone
    }
    val filterDStream: DStream[String] = mapDstream.filter(
      data =>
        data.contains("135")
    )
    filterDStream.print()
  }

  private def sexCount(kafkaData: InputDStream[ConsumerRecord[String, String]]) = {
    //TODO 3.女性多还是男性多
    val mapDStream: DStream[(String, Int)] = kafkaData.map {
      kdata =>
        val kafkaValue: String = kdata.value()
        val jsonData: Data = JSON.parseObject(kafkaValue, classOf[Data])

        (jsonData.sex, 1)
    }

    val reduceByKeyDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    reduceByKeyDStream.print()
  }

  private def groupName(kafkaData: InputDStream[ConsumerRecord[String, String]]) = {
    //TODO 2. 同一地区相同名字的共有几人
    val value: DStream[(String, String)] = kafkaData.map {
      kdata =>
        val kafkaValue: String = kdata.value()
        val data: Data = JSON.parseObject(kafkaValue, classOf[Data])
        (data.address, data.name)
    }
    val groupByKeyDStream: DStream[(String, Iterable[String])] = value.groupByKey()

    //beijing, [n1,n1,n3]
    val filterDstream: DStream[(String, Iterable[String])] = groupByKeyDStream.filter {
      data =>
        !data._2.equals(data._2)
    }
    filterDstream.saveAsTextFiles("D:\\bigdata\\result\\group")
  }

  private def getNameAndSum(kafkaData: InputDStream[ConsumerRecord[String, String]]) = {
    //TODO 1.相同名字的共有几人（√）
    val value: DStream[String] = kafkaData.map {
      kdata =>
        val kafkaValue: String = kdata.value()
        val data: Data = JSON.parseObject(kafkaValue, classOf[Data])
        data.name
    }
    val mapDStream: DStream[(String, Int)] = value.map(word => (word, 1))
    //val reduceByKeyDstream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val updateStateByKeyDstream: DStream[(String, Int)] = mapDStream.updateStateByKey(updateFunc)
    updateStateByKeyDstream.saveAsTextFiles("D:\\bigdata\\result\\result")
  }
}
