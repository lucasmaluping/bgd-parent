package com.feng.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")
    val line= ssc.socketTextStream("hadoop--1",10000)
    val lines=line.map(word =>(word,1))
    println(lines)
    val wordCount=lines.reduceByKeyAndWindow((a:Int,b:Int) =>(a+b),Seconds(15),Seconds(5))
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
