//package test
//
//import com.alibaba.fastjson.JSON
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka.KafkaCluster.Err
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
//import scalikejdbc.{DB, SQL}
//import scalikejdbc.config.DBs
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import util.{RequirementAnalyze, TimeUtils}
///**
//  * 将偏移量保存到MySQL中
//  */
//object SparkStreamingOffsetMysql {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession
//      .builder()
//      .appName("ssom")
//      .master("local[2]")
//      .getOrCreate()
//    val ssc = new StreamingContext(spark.sparkContext,Seconds(3))
//    ssc.checkpoint("D:\\abc\\APP")
//    //一系列基本的配置
//    val groupid = "gp1923"
//    val brokerList = "hadoop001:9092,hadoop002:9092,hadoop003:9092"
//    val topic = "ts"
//    //可能会有多个Topic
//    val topics = Set(topic)
//    //设置kafka的配置
//    val kafkas = Map(
//      "metadata.broker.list"->brokerList,
//      "group.id"-> groupid,
//      "auto.offset.reset"->kafka.api.OffsetRequest.SmallestTimeString
//    )
//   /*CREATE DATABASE bbs
//     CREATE TABLE `offsets` (
//      `id` int(11) NOT NULL AUTO_INCREMENT,
//      `topic` varchar(20) DEFAULT NULL,
//      `partitions` varchar(20) DEFAULT NULL,
//      `groupId` varchar(20) DEFAULT NULL,
//      `untilOffset` bigint(20) DEFAULT NULL,
//      PRIMARY KEY (`id`)
//    ) ENGINE=InnoDB AUTO_INCREMENT=115 DEFAULT CHARSET=utf8;
//    */
//    //加载配置
//    DBs.setup()
//    //这一块我们就不需要在进行查询ZK中的offset的了，直接查询MySQL中的offset数据
//    val fromdbOffset :Map[TopicAndPartition,Long] =
//      DB.readOnly{
//        implicit session =>
//          //查询每个分组下面的所有消息
//          SQL(s"select * from offsets where groupId ='${groupid}'")
//            //查询出来后，将MySQL中的数据赋值给这个元组
//            .map(m=>(TopicAndPartition(
//            m.string("topic"),m.int("partitions")),m.long("untilOffset")))
//            .toList().apply()
//      }.toMap //最后要toMap一下，因为前面的返回值已经给定
//    // 创建一个InputDStream，然后根据offset读取数据
//    var kafkaStream :InputDStream[(String,String)] = null
//    //从MySQL中获取数据，进行判断
//    if(fromdbOffset.size ==0){
//      //如果程序第一次启动
//      kafkaStream = KafkaUtils.
//        createDirectStream[String,String,StringDecoder,StringDecoder](
//        ssc,kafkas,topics)
//    }else{
//      //如果程序不是第一次启动
//      //首先获取Topic和partition、offset
//      var checckOffset = Map[TopicAndPartition,Long]()
//      // 加载kafka的配置
//      val kafkaCluster = new KafkaCluster(kafkas)
//      //首先获取Kafka中的所有Topic partition offset
//      val earliesOffsets: Either[Err,
//        Map[TopicAndPartition, KafkaCluster.LeaderOffset]] =
//        kafkaCluster.getEarliestLeaderOffsets(fromdbOffset.keySet)
//      //然后开始进行比较大小，用MySQL中的offset和kafka的offset进行比较
//      if(earliesOffsets.isRight){
//        //取到我们需要的Map
//        val topicAndPartitionOffset:
//          Map[TopicAndPartition, KafkaCluster.LeaderOffset] =
//          earliesOffsets.right.get
//        // 来个比较直接进行比较大小
//        checckOffset = fromdbOffset.map(owner=>{
//          //取我们kafka汇总的offset
//          val topicOffset = topicAndPartitionOffset.get(owner._1).get.offset
//          //进行比较  不允许重复消费 取最大的
//          if(owner._2 > topicOffset){
//            owner
//          }else{
//            (owner._1,topicOffset)
//          }
//        })
//      }
//      //不是第一次启动的话，按照之前的偏移量继续读取数据
//      val messageHandler = (mmd:MessageAndMetadata[String,String])=>{
//        (mmd.key(),mmd.message())
//      }
//      kafkaStream = KafkaUtils.
//        createDirectStream[String,String,
//        StringDecoder,StringDecoder,
//        (String,String)](ssc,kafkas,checckOffset,messageHandler)
//    }
//
//    // 获取province数据并广播
//    val provinceInfo = spark.sparkContext
//      .textFile("D:\\abc\\province.txt")
//      .collect().map(t => {
//      val arr = t.split(" ")
//      (arr(0), arr(1))
//    }).toMap
//    val provinceInfoBroadcast = spark.sparkContext.broadcast(provinceInfo)
//    //开始处理数据流，跟咱们之前的ZK那一块一样了
//    kafkaStream.foreachRDD(kafkaRDD=>{
//      //首先将获取的数据转换 获取offset  后面更新的时候使用
//      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      val baseData = kafkaRDD.map(t=>JSON.parseObject(t._2)) //获取实际的数据
//          .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
//          .map(jsobj=>{
//             val rechargeRes = jsobj.getString("bussinessRst") // 充值结果
//             val fee: Double = if (rechargeRes.equals("0000")) // 判断是否充值成功
//              jsobj.getDouble("chargefee") else 0.0 // 充值金额
//             val feeCount = if (!fee.equals(0.0)) 1 else 0 // 获取到充值成功数,金额不等于0
//             val starttime = jsobj.getString("requestId") // 开始充值时间
//             val recivcetime = jsobj.getString("receiveNotifyTime") // 结束充值时间
//             val pcode = jsobj.getString("provinceCode") // 获得省份编号
//             val province = provinceInfoBroadcast.value.get(pcode).toString // 通过省份编号进行取值
//            // 充值成功数
//            val isSucc = if (rechargeRes.equals("0000")) 1 else 0
//            // 充值时长
//            val costtime = if (rechargeRes.equals("0000")) TimeUtils.costtime(starttime, recivcetime) else 0
//            (starttime.substring(0, 8), // 年月日
//              starttime.substring(0, 10), // 年月日时
//              List[Double](1, fee, isSucc, costtime.toDouble, feeCount), // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
//              province, // 省份
//              starttime.substring(0, 12), // 年月日时分
//              (starttime.substring(0, 10), province) // (年月日时，省份)
//            )
//          }).cache()
//      //需求一 业务概括
//      //1)统计全网的充值订单量, 充值金额, 充值成功数，充值平均时长
//      val result1 = baseData.map(t => (t._1, t._3)).reduceByKey((list1, list2) => {
//        //拉链操作:List(1,2,3) List(2,3,4) =>List((1,2),(2,3),(3,4))
//        (list1 zip list2).map(t => t._1 + t._2)
//      })
//      RequirementAnalyze.requirement01(result1)
//      //需求二 业务质量
//      // 需求二：业务质量
//      val result2 = baseData.map(t => (t._6, t._3)).reduceByKey((list1, list2) => {
//        list1.zip(list2).map(t => t._1 + t._2)
//      })
//      RequirementAnalyze.requirement02(result2)
//
//      // 需求三：充值订单省份 TOP10
//      val result3: RDD[(String, List[Double])] = baseData.map(t => (t._4, t._3)).reduceByKey((list1, list2) => {
//        list1.zip(list2).map(t => t._1 + t._2)
//      })
//      result3
//      RequirementAnalyze.requirement03(result3)
//
//      // 需求四：实时充值情况分布
//      // 要将两个list拉倒一起去，因为每次处理的结果要合并
//      val result4 = baseData.map(t => (t._5, t._3)).reduceByKey((list1, list2) => {
//        list1.zip(list2).map(t => t._1 + t._2)
//      })
//      RequirementAnalyze.requirement04(result4)
//      //更新偏移量
//      DB.localTx{
//        implicit session =>
//          //取到所有的topic  partition offset
//          for(os<-offsetRanges){
//            //            // 通过SQL语句进行更新每次提交的偏移量数据
//            //            SQL("UPDATE offsets SET groupId=?,topic=?,partitions=?,untilOffset=?")
//            //              .bind(groupid,os.topic,os.partition,os.untilOffset).update().apply()
//            SQL("replace into " +
//              "offsets(groupId,topic,partitions,untilOffset) values(?,?,?,?)")
//              .bind(groupid,os.topic,os.partition,os.untilOffset)
//              .update().apply()
//          }
//      }
//    })
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
//
