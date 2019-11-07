import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
//由于输入文件有多个，产生不同的分区，为了生成序号，使用HashPartitioner将中间的RDD归约到一起
object FileSort {
  def main(arg:Array[String])={
    val conf = new SparkConf().setAppName("FileSort").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\IDEA\\DemoProject\\DATA\\FileSort\\file*")
    var num=0
    val result = lines.filter(line=>line.trim().length>0)
      .map(line=>(line.trim.toInt,""))
      .partitionBy(new HashPartitioner(1))
      .sortByKey(true)
      .foreach(x=>{
        num =num+1
        println(num+"\t"+x._1)
      })
    //result.saveAsTextFile("D:\\IDEA\\DemoProject\\DATA\\FileSort\\result")
  }
}
