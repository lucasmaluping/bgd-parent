package com.feng.spark.sql

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Text02 {
  def main(args: Array[String]) {
    //可以在本地运行。
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    val ssc = new StreamingContext(conf, Seconds(1))
    //创建topic
    val brobrokers = "192.168.111.129:9092,192.168.111.130:9092,192.168.111.131:9092"
    val sourcetopic = "fengzipeng";
    // val targettopic="target";
    //创建消费者组
    var group = "con-consumer-group"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      // "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      // "enable.auto.commit" -> (false: java.lang.Boolean)
    );
    //ssc.sparkContext.broadcast(pool)
    //创建DStream，返回接收到的输入数据
    val stream= KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(sourcetopic), kafkaParam))
    print("aaaaa")
    // stream.print()
    //这是从Kafka中拿出来的数据。此时的数据是这样的。
    //ConsumerRecord(topic = source, partition = 0, offset = 19, CreateTime = 1576116249732, checksum = 1915969165, serialized key size = -1, serialized value size = 2, key = null, value = ds)
    //ConsumerRecord(topic = source, partition = 0, offset = 20, CreateTime = 1576116250259, checksum = 2371880960, serialized key size = -1, serialized value size = 1, key = null, value = d)
    //我们只需要value值
    //如果认为这是个对象调用方法的话。那么这个方法会不断运行。或者说每隔一秒运行一次。
    val streams:DStream[ABC]=stream.map{record=>
      val jsonStr:String=record.value()
      //这部是为了将value值映射到对应的case类中
          val abc:ABC= JSON.parseObject(jsonStr,classOf[ABC])
           abc
    }
    //筛选数据只保留 area为shanghai的、type为event的数据，那么这个方法会每隔一秒运行一次，需要注意的是他会等上一个rdd形成之后。
    val str=streams.transform( rdd=>
       rdd.filter( abc =>
         (abc.area)=="shangdu"))
      //    val filter=streams.filter{ ABC => (ABC.area)="shanghai"
     //
     //}
    //}
    //统计每两分钟内的人数。
    //这里使用窗口函数。每30秒统计一次，间隔10秒
    val person=stream.window(Seconds(30),Seconds(10))
   // person.filter()
    person.count().print()+"...................................."
    str.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    //str.saveAsTextFiles("d:/huawei.txt")
  }
}