package com.bgd.thomas.application

import java.util.Properties

import com.bgd.thomas.dao.{FirstCleanoutCountDemo, SecondCleanoutCountDemo, ThirdCleanoutDemo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Thomas:侯佳伟，张瑜，张政，云宇庭，王强，杨福长，于浩
  * @date 2019/12/16 9:21
  * @version 1.0
  */
object SparkSqlDemo {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val liens: RDD[String] = sc.textFile("D:\\logs\\logger\\logger.log")


    val conn = new Properties()
    conn.put("user", "root")
    conn.put("password", "403411")
    conn.put("driver", "com.mysql.cj.jdbc.Driver")

    val url = "jdbc:mysql://localhost/spark?characterEncoding=utf-8&serverTimezone=UTC"

    //    liens.foreach(println(_))
    val values: RDD[String] = liens.filter(_.contains("vs"))
    values.foreach(println(_))
//    //将使用ios系统的用户，和使用andriod的用户统计出来
    //1.将用户使用的手机版本统计出来，做为表数据
    val rowRdd1: RDD[Row] = values.map(arr => {
      val values: Array[String] = arr.split(",")
      values(2)
    }).map(arr => {
      val os: Array[String] = arr.split("\"")
      (os(3), 1)
    }).reduceByKey(_ + _).map(arr => {
      FirstCleanoutCountDemo(arr._1, arr._2)
    }).map(arr => Row(arr.data, arr.ratio.toInt))
    rowRdd1.foreach(println(_))
    //2.创建sql表结构
    val structType1 = StructType {
      Array(
        StructField("os", StringType, false),
        StructField("num", IntegerType, false)
      )
    }
    //3.将数据写入sql数据库
    val df1: DataFrame = ssc.createDataFrame(rowRdd1, structType1)
    //
    val table1 = "os_table"
    df1.write.mode("overwrite").jdbc(url, table1, conn)


    //ios的用户APP的版本
    val rowRdd2: RDD[Row] = values.filter(_.contains("ios")).map(arr => {
      val liens: Array[String] = arr.split(",")
      liens(7)
    }).map(arr => {
      val vs: Array[String] = arr.split("\"")
      (vs(3), 1)
    }).reduceByKey(_ + _).map(arr => {
      SecondCleanoutCountDemo(arr._1, arr._2)
    }).map(arr => Row(arr.data, arr.ratio.toInt))
    rowRdd2.foreach(println(_))
    val structType2 = StructType {
      Array(
        StructField("vs", StringType, false),
        StructField("num", IntegerType, false)
      )
    }

    val df2: DataFrame = ssc.createDataFrame(rowRdd2, structType2)
    val table2 = "ios_table"
    df2.write.mode("overwrite").jdbc(url, table2, conn)

    //andriod的用户APP的版本
    val rowRdd3: RDD[Row] = values.filter(_.contains("andriod")).map(arr => {
      val liens: Array[String] = arr.split(",")
      (liens(7))
    }).map(arr => {
      val vs: Array[String] = arr.split("\"")
      (vs(3), 1)
    }).reduceByKey(_ + _).map(values => {
      ThirdCleanoutDemo(values._1, values._2)
    }).map(arr => Row(arr.data, arr.ratio.toInt))
    rowRdd3.foreach(println(_))

    val structType3 = StructType {
      Array(
        StructField("vs", StringType, false),
        StructField("num", IntegerType, false)
      )
    }

    val df3: DataFrame = ssc.createDataFrame(rowRdd3, structType3)
    val table3 = "Android_Table"
    df3.write.mode("overwrite").jdbc(url, table3, conn)


    sc.stop()
  }
}
