import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class AccesslogSchema(
                            ipAddress: String, // 客户端ip地址
                            clientIndentd: String, //客户端标识符
                            userId: String, //用户ID
                            dateTime: String, //时间
                            method: String, //http请求方式
                            url: String, //用户访问URL
                            protocol: String, //协议
                            responseCode: Int, //网站返回Http响应码
                            flux: Long //访问流量
                          )

object AccesslogSchema {
  def parseLog(line: String): AccesslogSchema = {
    val logArray = line.split("#")
    println(logArray.mkString(","))
    val method_url_protocol = logArray(4).split(" ")
    AccesslogSchema(logArray(0), logArray(1), logArray(2), logArray(3), method_url_protocol(0), method_url_protocol(1), method_url_protocol(2), logArray(5).toInt, logArray(6).toLong);
  }
}

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("LogAnalysis")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val logsRDD = sc.textFile("D://IDEA/DemoProject/DATA/rdd_statproject_AccessLog.txt")
      .map { line => AccesslogSchema.parseLog(line) }
      .cache()//后面几个统计都是基于该RDD继续计算，cache缓存不必每次从头计算，节省计算资源
    //统计1：单条记录的最大流量、最小流量和平均流量。
    val flux = logsRDD.map { log => log.flux }
    println("【单条记录最大流量】：" + flux.max())
    //【单条记录最大流量】：2968
    println("【单条记录最小流量】：" + flux.min())
    //【单条记录最小流量】：456
    val totalFlux = flux.reduce(_ + _)
    println("【流量总计】：" + totalFlux)
    //【流量总计】：13967
    val recordCount = flux.count()
    println("【访问记录数】：" + recordCount)
    //【访问记录数】：7
    println("【平均流量】：" + totalFlux/recordCount)
    //【平均流量】：1995
    //统计2：出现次数TopN的http响应码列表
    val topNRespCode = logsRDD.map { log => (log.responseCode, 1) }
      .reduceByKey(_ + _)
      .map(result => (result._2, result._1))
      .sortByKey(false)//倒序
      .take(2)//topN
    for (tuple <- topNRespCode) {
      println("【Http响应码 】：" + tuple._2 + "，【出现次数】：" + tuple._1);
    }
    //【Http响应码 】：200，【出现次数】：3
    //【Http响应码 】：404，【出现次数】：2
    //统计3：找出出现超出N次的客户端IP，有可能是恶意攻击IP
    val result = logsRDD.map { log => (log.ipAddress, 1) }
      .reduceByKey(_ + _)
      .filter(result => result._2 > 3) // > 找出出现超出N次的客户端IP，有可能是恶意攻击IP，与topN的区别是不用排序，节省性能
    for (tuple <- result) {
      println("ip : " + tuple._1 + "  出现次数：" + tuple._2);
    }
    //ip : 10.118.2.153  出现次数：4

    //用完记得释放cache
    logsRDD.unpersist(true)
  }
}
