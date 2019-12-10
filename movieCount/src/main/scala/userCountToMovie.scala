import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * 组7：杨慧慧，尹晓蓉，尹相予，刘洪涛，韩月梅，武彩艳
 * 哪个地区票房高即哪个地区用户多
 */
object userCountToMovie {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("userCountToMovie").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    val lineRDD: RDD[Array[String]] = sc.textFile("E:\\data\\users.dat").map(_.split("::"))
    //邮政编码第一位相同认为是一个地区
    val personRDD = lineRDD.map(x => User(x(0).toInt,x(1),x(2).toInt,x(3),x(4).substring(0,1)))
    import sqlc.implicits._
    val personDF: DataFrame = personRDD.toDF()
    personDF.show()
    personDF.registerTempTable("t_user")
    var sql="select zipcode,count(userId) as userCount from t_user group by zipcode"
    val res = sqlc.sql(sql)
    res.show()
    sc.stop()
  }
  //用户id，性别，年龄，职业，邮政编码     2::M::56::16::70072
  case class User(userId:Int,gender:String,age:Int,job:String,zipcode:String)
}

