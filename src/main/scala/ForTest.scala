import org.apache.spark.sql.SparkSession
object ForTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("for-test")
      .master("local")
      .getOrCreate()
    val word = Array("hello","i","am","zoe")
    val sc = spark.sparkContext
//    val wordRdd = sc.parallelize(word).flatMap(x=>x.split(" ")).collect.foreach(println)
    val wordRdd = sc.parallelize(word).map(x=>x.split(" ")).foreach(x=>println(x.mkString))
//    val rdd =sc.parallelize(Array("a=b","c=d","e=f"))
//    rdd.foreach(println)
//    println("-----------------------------------------------")
//    val map_rdd = rdd.map(_.split("=")).foreach(x=>println(x.mkString+","))
//    println("-----------------------------------------------")
//    val flatmap_rdd = rdd.flatMap(_.split("=")).foreach(x=>println(x.mkString(",")))

  }

}
