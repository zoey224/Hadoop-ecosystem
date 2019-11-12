import org.apache.spark.sql.SparkSession
object DropColumns {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame Drop-Columns")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA"),
      (6, "Widget Co", 12000.00, 10.00, "AZ")
    )
    val customerRows= spark.sparkContext.parallelize(custs,4)
    val customerDF = customerRows.toDF("id","name","sales","discount","state")
    println("*** Here's the whole DataFrame")
    customerDF.printSchema()
    customerDF.show()

//    去掉其中几列
    val fewCols = customerDF.drop("sales","state")
    fewCols.show()

//    去掉完全重复的行
    val withoutDuplicates = customerDF.dropDuplicates()
    withoutDuplicates.show()
//   去掉指定列完全相同的行
    val withoutPartials = customerDF.dropDuplicates(Seq("name","sales","state"))
    withoutPartials.show()
  }
}
