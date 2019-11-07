package com.tian.yi
import org.apache.spark.{SparkContext,SparkConf}

//class SecondarySortKey(val first:Int, val second:Int) extends Ordered[SecondarySortKey] with Serializable {
//  override def compare(other:SecondarySortKey):Int={
//    if(this.first-other.first!=0){
//      this.first-other.first
//    }else{
//      this.second-other.second
//    }
//  }
//}

object SecondarySort {
  def main(args: Array[String]):Unit= {
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local")
    val sc = new SparkContext(conf)
    val lines =sc.textFile("D:\\IDEA\\DemoProject\\DATA\\SecondarySort\\file*")
    val pairWithSortKey = lines
      .map(line=>(new SecondarySortKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line))
    val sorted = pairWithSortKey.sortByKey(false)
    val sortedResult =sorted.map(sortedLine=>sortedLine._2)
    sortedResult.collect().foreach(println)

  }

}
