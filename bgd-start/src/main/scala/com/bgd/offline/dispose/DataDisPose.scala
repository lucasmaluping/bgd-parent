package com.bgd.offline.dispose

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.bgd.offline.bean.Data
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}



/**
  * @Author :star
  * @Date :2019/12/11 9:01
  * @Version :1.0
  */
object DataDisPose {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DataDisPose").setMaster("local[*]")
    val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sinkMysqlIsArAndNa(ss)
  }

  private def selHiveData(): Unit = {
    //TODO 5.用hql语句对每一个字段进行聚合操作
    /**
      * 例：
      * 1.查询手机号开头的为135
      * select * from bigdata where phone like '%135%';
      * 2.查询性别为男的
      * select * from bigdata where sex = "男";
      * 3.查询住在商都的人的姓名
      * select * from bigdata where address = "二连浩特)";
      */

  }

  private def saveHive(ss: SparkSession) = {
    //TODO 4.将所有的数据备份到hive
    /**
      * hive 创表语句
      * create external table bigdata(name String,sex String,phone String,address String)row format delimited
      * fields terminated by ','
      * location '/spark/data.log';
      */
    val data: RDD[String] = ss.sparkContext.textFile("dataInput/logger.json")
    val mapRDD: RDD[(String, String, String, String)] = data.map {
      row =>
        val jsonData: Data = JSON.parseObject(row, classOf[Data])
        (jsonData.name, jsonData.sex, jsonData.phone, jsonData.address)
    }
    mapRDD.saveAsTextFile("hdfs://hdp-1:9000/spark/data.log")
  }

  private def saveHDFS(ss: SparkSession) =  {
    //TODO 3.将所有的数据通过代码的形式备份到hdfs上
    val data: RDD[String] = ss.sparkContext.textFile("dataInput/logger.json")
    data.saveAsTextFile("hdfs://hdp-1:9000/spark")
  }

  private def groupByPhone(ss: SparkSession) = {
    //TODO 2.对phone进行归属地划分，保存到文件中
    val data: RDD[String] = ss.sparkContext.textFile("dataInput/logger.json")
    val mapRDD: RDD[(String, String, String, String)] = data.map {
      row =>
        val jsonData: Data = JSON.parseObject(row, classOf[Data])
        (jsonData.phone.substring(0, 3), jsonData.address, jsonData.sex, jsonData.name)
    }
    val groupByRDD: RDD[(String, Iterable[(String, String, String, String)])] = mapRDD.groupBy(x => x._1)

    groupByRDD.saveAsTextFile("dataoutput/")

    /*import ss.implicits._

    val df: DataFrame = groupByRDD.toDF("phone","address","sex","name")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    df.write.jdbc(
      "jdbc:mysql://localhost:3306/frame?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true",
      "bigdata3",
      prop
    )
*/
  }

  private def sinkMysqlIsArAndNa(ss: _root_.org.apache.spark.sql.SparkSession) = {
    // TODO 1.将address+name保存到mysql中
    //val df: DataFrame = ss.read.json("dataInput/logger.json")
    val data: RDD[String] = ss.sparkContext.textFile("C:/zpark/bgd-parent/bgd-start/dataInput/logger.jsonn")
    val mapRDD: RDD[(String, String)] = data.map {
        row =>
          val jsonData: Data = JSON.parseObject(row, classOf[Data])
          (jsonData.address, jsonData.name)
    }
    import ss.implicits._
    val df: DataFrame = mapRDD.toDF("address", "name")

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "lucas")
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    df.write.jdbc(
      "jdbc:mysql://localhost:3306/frame?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true",
      "bigdata1",
      prop
    )
  }
}
