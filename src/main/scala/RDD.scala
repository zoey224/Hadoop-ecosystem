import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object RDD {
  def pairMapRDD(sc:SparkContext):Unit ={
    val rdd= sc.makeRDD(List(("a", 15), ("b", 6), ("c", 2), ("a", 7)))
    val other = sc.parallelize(List(("a", 3)), 1)

    val rddReduce1= rdd.reduceByKey((x, y) => x + y)
    println("【reduceByKey1】：" + rddReduce1.collect().mkString(","))//

    val rddReduce2= rdd.reduceByKey(_ + _)
    println("【reduceByKey2】：" + rddReduce2.collect().mkString(","))//

    val rddGroup = rdd.groupByKey()
    println("【groupByKey】：" + rddGroup.collect().mkString(","))//

    val rddKey = rdd.keys
    println("【rddkeys】"+rddKey.collect().mkString(","))

    val rddValue= rdd.values
    println("【rddvalues】"+rddValue.collect().mkString(","))

    val rddSortAscend: RDD[(String, Int)] = rdd.sortByKey(true, 1)
    println("【rddSortAscend】：" + rddSortAscend.collect().mkString(",")) //

    val rddSortDescend: RDD[(String, Int)] = rdd.sortByKey(false, 1)
    println("【rddSortDescend】：" + rddSortDescend.collect().mkString(",")) //


  }
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .appName("exercise for RDD")
      .master("local")
      .getOrCreate()
    val sc =spark.sparkContext
    sc.setLogLevel("ERROR")
    pairMapRDD(sc)
    sc.stop()
  }


}
