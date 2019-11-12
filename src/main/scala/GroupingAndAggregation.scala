import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column,SparkSession}
object GroupingAndAggregation {
  case class Cust(id:Integer, name:String, sales:Double, discount:Double, state:String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Group and agg")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val custs =Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA") ,
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val DF = spark.sparkContext.parallelize(custs,4).toDF()
    DF.show()
//    DF.groupBy("state").agg("discount"->"max").show()
  }
}
