import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *魏鹏程，张帅，温凯华，冯瑞，白瑞福，谢瑞
  */

//用户Id，电影Id，电影评分，评分时间戳
case class Movies(userid: Int,movieid: String,rating: Double,timestamped: String)

object MoviesTop5 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MoviesTop5")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("file:///F:/data/ratings.dat")
    import  spark.implicits._
    val words: Dataset[Array[String]] = lines.map(_.split("::"))
    val values: Dataset[Movies] = words.map(x => Movies(x(0).toInt,x(1).toString,x(2).toDouble,x(3).toString))
    val df: DataFrame = values.toDF()
    val rs: Unit = df.createTempView("movies")
//    val useSql: DataFrame = spark.sql("select top 5 userid,movieid,rating,timestamped from movies order by rating desc")
//    val userSql: DataFrame = spark.sql("select movieid,max(rating) from movies group by movieid")

    /**
      * 根据电影评分统计各评分电影点击总和并将数据保存到HDFS中
      */
    val userSql: DataFrame = spark.sql("select rating,count(*) as movieCount from movies group by rating order by movieCount desc")
    userSql.write.format("csv").save("/AJ.txt")
    userSql.show()

    spark.stop()

  }


}
