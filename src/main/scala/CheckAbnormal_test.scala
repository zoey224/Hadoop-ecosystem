package tianyi
// 网络入侵检测数据集 1999 KDDcup
/*
1998 年林肯实验室建立了模拟美国空军局域网的一个网络环境，
收集了9 周时间的网络链接和系统审计数据，仿真各种用户类型、
各种不同的网络流量和攻击手段，使它就像一个真实的网络环境。
 对以上的数据集进行处理，形成了一个新的数据集。
 该数据集用于1999 年举行的KDDCUP 竞赛中，成为著名的KDD99 数据集。
 */

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import tianyi.CheckAbnormal.anomalies
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object CheckAbnormal_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    val dataPath = "C:\\Users\\admin\\Desktop\\kddcup.data\\kddcup10.data"
    val rawData = sc.textFile(dataPath)
    // 取数据集的最后一个字段分类降序统计
    println(rawData.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse)

    }




//  def clusteringTake1(rawData: RDD[String]) = {
//    //分类统计各种情况
//    rawData.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
//
//
//    val labelsAndData = rawData.map {
//      line =>
//        //将csv格式的行拆分成列，创建一个buffer，是一个可变列表
//        val buffer = line.split(",").toBuffer
//        //删除下标从1开始的三个类别型列
//        buffer.remove(1, 3)
//        //删除下标最后的标号列
//        val label = buffer.remove(buffer.length - 1)
//        //保留其他值并将其转换成一个数值型(Double型对象)数组
//        val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
//        //将数组和标号组成一个元祖
//        (label, vector)
//    }
//    val data = labelsAndData.values.cache()
//    val kmeans = new KMeans()
//    val model = kmeans.run(data)
//    model.clusterCenters.foreach(println)
//    val clusterLabelCount = labelsAndData.map {
//      case (label, datum) =>
//        //预测样本datum的分类cluster
//        val cluster = model.predict(datum)
//        (cluster, label)
//    }.countByValue()
//    clusterLabelCount.toSeq.sorted.foreach {
//      case ((cluster, label), count) =>
//        println(f"$cluster%1s$label%18s$count%8s")
//    }
//    data.unpersist()
//  }
}