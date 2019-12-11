package com.bgt.disizu

import org.apache.spark.sql.SparkSession

/*
  第四组：郭义龙，刘富强，沈岩，吴仔健，张娜，李金超，尹佳欣，冯子鹏
    1、数据源格式： {"banji":"one","name":"xincongming","chengji":99}
要求：根据班级分组，求各班级总的平均分以及成绩TOP5
 */
object Test01 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val test=spark.read.json("hdfs://hadoop--4:9000/text.jsno")
    //查看里面的数据
    test.show()
    //注册成表结构。
    test.createOrReplaceTempView("test")
    //根据班级分组，求各班级总的平均分以及成绩TOP5
    val sql=spark.sql("SELECT avg(chengji)as average FROM test group by banji")
    sql.show()
  }
}
