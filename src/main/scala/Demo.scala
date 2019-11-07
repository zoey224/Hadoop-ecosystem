import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.{SQLContext, Row,DataFrame}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SparkSession basic example")
      .getOrCreate()
// spark 读取.csv文件 以DataFrame为操作方式
    val csvDF = spark.read.format("csv")
      .option("sep", "\t")
      .option("header", "false")
      .load("C:\\Users\\admin\\Desktop\\taxi.csv")
    csvDF.printSchema()
    csvDF.show(10)


// 注意mysql jdbc的驱动的版本 5.0  com.mysql.jdbc.Driver, 8.0 com.mysql.cj.jdbc.Driver
    val mysqlDF: DataFrame = spark.read.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/localtest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
  //.option("servertimezone","UTC")
  .option("dbtable","auto_run_test")
  .option("user","root")
  .option("password","20191017")
  .option("driver","com.mysql.cj.jdbc.Driver").load()
    mysqlDF.show()


  }
}