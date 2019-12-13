package com.xin.operation

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求： 统计一周内活跃用户（一周内登陆次数 >=7 次）
 * 组5：{辛佳楠，李东，索杰敏，谢子文，于帅鹏，李万莹，高璐，张征，王艺嘉}
 *
 * 1、收集数据，sparkstreaming实时进行过滤，只保留mid，time
 * 2、将过滤后数据发送kafka，flume消费kafka数据，下沉hdfs
 * 3、sparksql或者hive进行业务处理
 */


object SqlOperation {
  def main(args: Array[String]): Unit = {
    val sc  =new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))

    //创建SQLContext对象
    val sqlc: SQLContext = new SQLContext(sc)
    val lineRDD: RDD[Array[String]] = sc.textFile("com/xin/active.txt").map(_.split(","))
    //将获取数据 关联到样例类中  Person自定义的类
    val personRDD: RDD[Data] = lineRDD.map(x => Data(x(0),x(1).toInt))

    //toDF相当于反射，这里若要使用的话，需要导入包，且必须放在第一个toDF上面
    import sqlc.implicits._
    val personDF: DataFrame = personRDD.toDF()

    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date: String = dateFormat.format(now)

    //    二、使用Sql语法
    //注册临时表，这个表相当于存储在 SQLContext中所创建对象中
    personDF.registerTempTable("t_data")
    val sql = "select id,count(time) num from t_data where time>"+date+"group by id having num>7"
    //查询
    val res: DataFrame = sqlc.sql(sql)
    res.show()  //默认打印是20行
    res.write.mode("append").format("csv").save("/AJ.txt")
  }

  //case class,编译器自动为你创建class和它的伴生 object，并实现了apply方法让你不需要通过 new 来创建类实例
  case class Data(id:String,time:Long)
}


