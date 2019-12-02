//method 1
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path}
object isFileExists {
  def main(args: Array[String]): Unit = {
    val config = new Configuration()
    val fs = FileSystem.get(new URI("adress"),config)
    val path = new Path("/*.txt")
    if (fs.exists(path)){
      println("exists")
    }else {
      println("Not exists")
    }
  }
}



//method 2

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
.appName("is file exists")
.master("local")
.getOrCreate()
val sc = spark.sparkContext
val conf = sc.hadoopConfiguration
val fs = org.apache.hadoop.fs.FileSystem.get(conf)
val exists =  fs.exists(new org.apache.hadoop.fs.Path("address"))
if (exists){
  println("exists")
}else {
  println("Not exists")
}
