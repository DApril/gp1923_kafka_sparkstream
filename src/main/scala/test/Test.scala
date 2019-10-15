package test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val clicklog = Seq(
       "INFO 2016-07-25 00:29:53 requestURl:/click?app=0&p=1&did=18005472&industry=469&adid=31",
       "INFO 2016-07-25 00:29:53 requestURl:/click?app=0&p=2&did=18005472&industry=469&adid=31",
       "INFO 2016-07-25 00:29:53 requestURl:/click?app=0&p=1&did=18005472&industry=469&adid=32",
       "INFO 2016-07-25 00:29:53 requestURl:/click?app=0&p=1&did=18005472&industry=469&adid=33")
    val imp = Seq(
      "INFO 2016-07-25 00:29:53 requestURl:/imp?app=0&p=1&did=18005472&industry=469&adid=31",
      "INFO 2016-07-25 00:29:53 requestURl:/imp?app=0&p=2&did=18005472&industry=469&adid=31",
      "INFO 2016-07-25 00:29:53 requestURl:/imp?app=0&p=1&did=18005472&industry=469&adid=34"
    )
    val clickRDD: RDD[String] = sc.parallelize(clicklog)
    val impRDD: RDD[String] = sc.parallelize(imp)
    val rdd1: RDD[String] = clickRDD.map(log => {
      val strs = log.split("&|\\?|/")
      val strs1 = strs(6).split("=")
      strs1(1)
    })

    val rdd2: RDD[String] = impRDD.map(log => {
      val strs = log.split("&|\\?|/")
      val strs1 = strs(6).split("=")
      strs1(1)
    })

    val rdd3 = rdd1.map((_,1)).reduceByKey(_+_)
    val rdd4 = rdd2.map((_,1)).reduceByKey(_+_)
    val rdd5: RDD[(String, (Option[Int], Option[Int]))] = rdd3.fullOuterJoin(rdd4)
    val rdd6 = rdd5.mapValues(x=>{
       if(x._1.isEmpty){
         (0,x._2.get)
       }
       else if(x._2.isEmpty){
         (x._1.get,0)
       }else (x._1.get,x._2.get)
    })
    println(rdd6.collect().toBuffer)
    sc.stop()
  }
}
