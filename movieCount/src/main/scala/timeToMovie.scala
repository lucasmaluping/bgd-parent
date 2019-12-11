
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
 * 组7：杨慧慧，尹晓蓉，尹相予，刘洪涛，韩月梅，武彩艳
 * 需求：电影的评分时间点分布   哪个地区票房高即哪个地区用户多
 */
object timeToMovie {//电影的评分时间点分布
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("timeToMovie").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    val lineRDD: RDD[Array[String]] = sc.textFile("E:\\data\\ratings.dat").map(_.split("::"))
    val personRDD = lineRDD.map(x => Comment(x(0).toInt,x(1).toInt,x(2).toDouble,x(3)))
    import sqlc.implicits._
    val personDF: DataFrame = personRDD.toDF()
   // personDF.show()
    personDF.registerTempTable("t_comment")
    var sql="select FROM_UNIXTIME(ctime,'yyyy-MM-dd HH:mm:ss') as ftime ,count(comment) as commentCount from t_comment group by ftime"
    val res = sqlc.sql(sql)
    res.show()
    sc.stop()
  }
  //用户ID，电影ID，评分，评分时间戳   1::1193::5::978300760
  case class Comment(userId:Int,movieId:Int,comment:Double,ctime:String)
}

