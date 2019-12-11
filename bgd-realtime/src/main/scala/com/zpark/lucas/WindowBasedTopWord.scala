package com.zpark.lucas

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author Lucas
  * @Date 2019/12/11 13:38
  * @Version 1.0
  *         测试窗口函数reduceByKeyAndWindow
  */

object WindowBasedTopWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WindowBasedTopWord")
    conf.setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("d:/checkpoint")
    val words: ReceiverInputDStream[String] = ssc.socketTextStream("hdp-1", 8888)
    val pairs: DStream[(String, Int)] = words.flatMap(_.split(" ")).map(x => (x, 1))
    pairs.foreachRDD(rdd => {
      println("--------------split RDD begin--------------")
      rdd.foreach(println)
      println("--------------split RDD end--------------")
    })

    val pairsWindow: DStream[(String, Int)] = pairs.reduceByKeyAndWindow(_+_, _-_,Seconds(20),Seconds(10))
    val sortDstream: DStream[(String, Int)] = pairsWindow.transform(rdd => {
      val sortRdd: RDD[(String, Int)] = rdd.map(t => (t._2, t._1)).sortByKey(false).map(t => (t._2, t._1))
      val more: Array[(String, Int)] = sortRdd.take(3)
      println("--------------print top 3 begin--------------")
      more.foreach(println)
      println("--------------print top 3 end--------------")
      sortRdd
    })
    sortDstream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}


