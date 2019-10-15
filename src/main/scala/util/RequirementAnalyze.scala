package util

import org.apache.spark.rdd.RDD

object RequirementAnalyze {

  /**
    * 需求一：业务概况
    *
    * @param lines
    */
  def requirement01(lines: RDD[(String, List[Double])]): Unit = {
    // lines：（日期， List）
    lines.foreachPartition(part => {
      val jedis = JedisConnectionPool.getConntection()
      // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
      part.foreach(t => {
        // 充值订单数
        jedis.hincrBy(t._1, "total", t._2(0).toLong)
        // 充值总金额
        jedis.hincrByFloat(t._1, "money", t._2(1))
        // 充值成功数
        jedis.hincrBy(t._1, "success", t._2(2).toLong)
        // 充值总时长
        jedis.hincrBy(t._1, "time", t._2(3).toLong)
      })
      jedis.close()
    })
  }

  /**
    * 需求二：业务质量
    *
    * @param lines
    */
  def requirement02(lines: RDD[((String, String), List[Double])]): Unit = {
    lines.foreachPartition(f => {
      val jedis = JedisConnectionPool.getConntection()
      // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
      f.foreach(t => {
        jedis.hincrBy(t._1._1, t._1._2, t._2.head.toLong - t._2(2).toLong)
      })
      jedis.close()
    })
  }

  /**
    * 充值订单省份 TOP10
    */
  def requirement03(lines: RDD[(String, List[Double])]): Unit = {
    lines.sortBy(_._2.head, false)
      // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
      .map(t => (t._1, (t._2(2) / t._2.head * 100).formatted("%.1f")))
      .foreachPartition(t => {
        // 拿到连接
        val conn = JdbcMysql.getConn()
        t.foreach(t => {
          val sql = "insert into CityTopN(city,success) " +
            "values('" + t._1 + "'," + t._2.toDouble + ")"
          val state = conn.createStatement()
          state.executeUpdate(sql)
        })
        JdbcMysql.releaseCon(conn)
      })
  }

  /**
    * 需求四：实时充值情况分布
    *
    * @param lines
    */
  def requirement04(lines: RDD[(String, List[Double])]): Unit = {
    lines.map(t => {
      // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
      CountMoney.cases(List(t._1, t._2(1), t._2(4)))
    }).map(t => (t.map(_._1), t.map(t => (t._2, t._3))))
      .reduceByKey((list1, list2) => list1.zip(list2)
        .map(t => (t._1._1 + t._2._1, t._1._2 + t._2._2)))
      .foreachPartition(t => {
        // 拿到连接
        val conn = JdbcMysql.getConn()
        t.foreach(t => {
          val sql = "insert into realtime(hour,count,money) " +
            "values('" + t._1 + "','" + t._2.map(_._1.toInt) + "','" + t._2.map(_._2.toDouble) + "')"
          val state = conn.createStatement()
          state.executeUpdate(sql)
        })
        JdbcMysql.releaseCon(conn)
      })
  }

  /**
    * 需求五：充值请求
    *
    * @param lines
    */
  def requirement05(lines: RDD[((String, String), List[Double])], myDate: String): Unit = {
    lines.filter(_._1._1 == myDate).map(t => {
      // ((年月日, 省份)，List(数值1用于统计订单量, 充值成功数, 充值金额不等于0的充值成功数))
      (t._1, t._2(0) - t._2(1), ((t._2(0) - t._2(1)) / t._2(0) * 100).formatted("%.2f"))
    }).foreachPartition(part => {
      // 拿到连接
      val conn = JdbcMysql.getConn()
      part.foreach(t => {
        val sql = "insert into failtopn(time,province,failnumber,failrate) " +
          "values('" + t._1._1 + "','" + t._1._2 + "'," + t._2.toInt + "," + t._3.toDouble + ")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      JdbcMysql.releaseCon(conn)
    })
  }
}
