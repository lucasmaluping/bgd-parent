package com.reduceDa

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *杨慧慧，尹晓蓉，尹相予，韩月梅，刘洪涛，武彩艳
 */
object reduceData {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("reduceData").setMaster("local[1]")
    val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    callRecord(ss)
    //callCount(ss)
  }

  //TODO 1.统计156开头用户的通话记录保存到mysql中
  private def callRecord(ss: SparkSession) ={
    val data: RDD[String] = ss.sparkContext.textFile("dataInput/callRecord.log")
    val lineRdd: RDD[Array[String]] = data.map(_.split("\t"))
    val callRdd: RDD[Calllog] = lineRdd.map(x => Calllog(x(0), x(1), x(2), x(3)))
    import ss.implicits._
    val callDF: DataFrame = callRdd.toDF()
    callDF.registerTempTable("t_call")
    var sql="select * from t_call where call1 like '156%'"
    val res = ss.sql(sql)
    res.show()
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    res.write.jdbc(
      "jdbc:mysql://localhost:3306/frame?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true",
      "callRecord1",
      prop
    )


  }
  //TODO 2.统计某一时间点的通话记录,将结果保存到hdfs上
  private def callCount(ss:SparkSession)={
    val data: RDD[String] = ss.sparkContext.textFile("dataInput/callRecord.log")
    val lineRdd: RDD[Array[String]] = data.map(_.split("\t"))
    val callRdd: RDD[Calllog] = lineRdd.map(x => Calllog(x(0), x(1), x(2), x(3)))
    import ss.implicits._
    val callDF: DataFrame = callRdd.toDF()
    callDF.registerTempTable("t_call")
    var sql="select * from t_call where ctime = '20190103170827'"
    val res = ss.sql(sql)
    res.show()
    res.write.mode("append").format("csv").save("/spark/data.log")
  }

  case class Calllog(call1:String,call2:String,ctime:String,duration:String)
}
