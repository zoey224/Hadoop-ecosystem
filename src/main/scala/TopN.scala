import org.apache.spark.{SparkConf,SparkContext}
object TopN {
  def main(arg:Array[String]):Unit={
    //SparkConf负责管理所有Spark的配置项
    val conf = new SparkConf().setAppName("FindTopN").setMaster("local")
    //任何Spark程序都是从SparkContext开始的，SparkContext初始化需要一个SparkConf对象，包含Spark集群配置的各种参数
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines = sc.textFile("D:\\IDEA\\DemoProject\\DATA\\TopN\\file*")
    var num =0
    //line.trim()用于去掉字符串前后空格，例如“ ab c "执行后变为”ab c"
    val result = lines.filter(line=>(line.trim().length>0)&&(line.split(",").length==4))
      .map(_.split(",")(2))
      .map(x=>x.toInt)
      .sortBy(x=>x,false)
      .take(5)
      //sortByKey的写法：
      //.map(x=>(x.toInt,""))
      //.sortByKey(false)
      //.map(x=>x._1).take(5)
      .foreach(x=>{
        num=num+1
        println(num+"\t"+x)
      })
}
}
