import org.apache.spark.{SparkContext,SparkConf}
object MaxOrMin {
  def main(arg:Array[String]):Unit={
    val conf = new SparkConf().setAppName("FindMaxOrMin").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\IDEA\\DemoProject\\DATA\\MaxOrMin\\file*")
    //遍历
    val result = lines.filter(line=>line.trim().length>0)
      .map(line=>("key",line.trim.toInt))
      .groupByKey()
      .map(x=>{
        var max =Integer.MIN_VALUE
        var min =Integer.MAX_VALUE
        for(num<-x._2){
          if(num>max){
            max = num
          }
          if(num<min){
            min = num
          }
        }
        (max,min)
        }).collect.foreach(x=>{
      println("max:\t"+x._1)
      println("min:\t"+x._2)
    })
  }

}
