package com.bgd.offline.dispose

/**
  * @Author Lucas
  * @Date 2019/12/12 13:49
  * @Version 1.0
  *         scala保存数据到mysql
  */
import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//把每个jsp的访问量，直接保存到Oracle数据中
// spark-submit --master local --class SaveMySQL SaveSQL.jar
object SaveMySQL {
  def main(args: Array[String]): Unit = {
    //定义SparkContext对象
    val conf = new SparkConf().setAppName("SaveSQL").setMaster("local[*]")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val line: RDD[String] = sc.textFile("c:/data.log")
    //读入数据
    //日志：192.168.88.1 - - [30/Jul/2017:12:54:38 +0800] "GET /MyDemoWeb/hadoop.jsp HTTP/1.1" 200 242
    line.foreach(println)
    val logData: RDD[(String, Int)] = line.map(
      line => {
        //解析字符串：找到hadoo.jsp
        //得到: .jsp位置
        val index1 = line.indexOf("\"") //得到的是第一个双引号的位置
        val index2 = line.lastIndexOf("\"") //得到的是最后一个双引号的位置

        //子串：GET /MyDemoWeb/hadoop.jsp HTTP/1.1
        val line1 = line.substring(index1 + 1, index2)
        val index3 = line1.indexOf(" ") //子串中第一个空格
        val index4 = line1.lastIndexOf(" ") //子串中最后一个空格

        //子串：/MyDemoWeb/hadoop.jsp
        val line2 = line1.substring(index3 + 1, index4)

        //得到jsp名字: hadoop.jsp
        val jspPage = line2.substring(line2.lastIndexOf("/") + 1)

        //返回: 格式:  (hadoop.jsp,1)
        (jspPage, 1)
      }
    )
    logData.foreach(println)
//    logData.foreachPartition(println)
    logData.foreachPartition(saveToMySQL)
  }
  //create database sparktest
  //create table mytest( id INT AUTO_INCREMENT,url varchar(200),num INT,PRIMARY KEY(id));
  //定义一个函数，将每个分区中的数据保存到MySQL中
  // 注意上传 mysql-connector-java-5.1.47.jar 到 SPARK_HOME/jars/下面，否则找不到jar包
  def saveToMySQL(it: Iterator[(String, Int)]) = {
    // 加载 MySQL驱动类
    // 如果不添加 也可以自动加载
    //classOf[com.mysql.jdbc.Driver]
    // 格式
    //val conn_str = "jdbc:mysql://localhost:3306/DBNAME?user=DBUSER&password=DBPWD"
    // 定义继承配置
    val dbName: String = "mydbtest"
    val tableName:String = "mytest"
    val dbUser: String = "root"
    val dbPasswd: String = "lucas"
    val dbHost: String = "localhost"
    val dbPort: String = "3306"
    // 代码
    val conn_str = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName + "?useSSL=false&serverTimezone=UTC&user=" + dbUser + "&password=" + dbPasswd
    //jdbc:mysql://localhost:3306/frame?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true
    // 获取连接
    val conn: Connection = DriverManager.getConnection(conn_str)
    println("______________________________________________________________________________________________________")
    println("______________________________________________________________________________________________________")
    println("_________________________________get Result from table________________________________________________")
    println("______________________________________________________________________________________________________")
    println("______________________________________________________________________________________________________")
    // 读取当前数据
    try {
      // 配置为只读模式
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val QuerySql = "SELECT * FROM "+tableName
      // 执行查询语句
      val rs = statement.executeQuery(QuerySql)
      // 遍历获取到的结果
      while (rs.next) {
        println(rs.getString("url"))
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    println("______________________________________________________________________________________________________")
    println("______________________________________________________________________________________________________")
    println("_________________________________insert Data    ______________________________________________________")
    println("______________________________________________________________________________________________________")
    println("______________________________________________________________________________________________________")
    // 插入数据
    var pst: PreparedStatement = null
    var InsertSql = "insert into "+tableName+" (url,num) values(?,?)"
    try {
      pst = conn.prepareStatement(InsertSql)
      //保存数据
      it.foreach(data => {
        pst.setString(1, data._1)
        pst.setInt(2, data._2)
        pst.executeUpdate()
      })
    } catch {
      case e1: Exception => println("Some Error : " + e1.getMessage)
    } finally {
      if (pst != null) pst.close()
      if (conn != null) conn.close()
    }
  }
}

