import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.jblas.DoubleMatrix


object MovieAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MoiveLen Analysis").setMaster("local")
    val sc = new SparkContext(conf)
    val DIR="D://IDEA/DemoProject/DATA/ml-100k/"
    val rawData = sc.textFile(DIR +"u.data")
    println("rawData.first()="+rawData.first())
    //rawFirstData=196	242	3	881250949
    val rawRatings = rawData.map(_.split("\t").take(3))//提取评分数据前3条，即用户ID，电影ID，评分
    println("rawRatings.first()=" + rawRatings.first().mkString("\t"))
    //rawRatings.first()=196	242	3
    val ratings= rawRatings.map{case Array(user, movie, rating) => Rating(user.toInt,movie.toInt, rating.toInt)}
    println(ratings.first())
    val model = ALS.train(ratings, 50, 10, 0.01)
    val predictedRating = model.predict(789,123)  //预测用户789对电影123的评级
    println("predictedRating of user 789 to movie 123 is ",predictedRating)
    val userId =789
    val k = 10
    val movies = sc.textFile(DIR+"u.item")
    val titles = movies.map(line=>line.split("\\|").take(2)).map(array=>(array(0).toInt,array(1)))
      .collectAsMap()
    println("电影123的标题如下:")
    println(titles(123))
    //查看电影ID为123的电影标题
    val movieForUser = ratings.keyBy(_.user).lookup(789)
    println("用户789评分过的电影如下:")
    movieForUser.foreach(println)//打印用户789评分过的电影
    println("用户789评分过的电影好评top10如下:")
    movieForUser.sortBy(-_.rating).take(10).map(rating=>(titles(rating.product),rating.rating)).foreach(println)
    val topKRecs = model.recommendProducts(userId, k)
    println("用户789的前10个推荐列表如下:")
    topKRecs.map(rating => (titles(rating.product) , rating.rating)).foreach(println)//输出推荐列表

    //--------------物品推荐---------------------------------------------------------------------
    val itemId =567
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
    println(consineSimilarity(itemVector, itemVector))

  }
  //计算余弦相似度
  def consineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix):Double={
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
  /***
   * * 计算APK（K值平均准确率）
   * * @param actual
   * * @param predicted
   * * @param k
   * * @return
   ***/
  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex){
      if(actual.contains(p)){
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if(actual.isEmpty){
      1.0
    }    else{
      score / scala.math.min(actual.size, k).toDouble
    }
  }


}
