package com.hbzq.bigdata.spark.operator.rdd

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.utils.{JsonUtil, RedisUtil, RuleVaildUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
class TsscjOperator(var rdd: RDD[String],
                    var exchangeMapBC: Broadcast[Map[String, BigDecimal]]) extends RddOperator {

  override def compute(): Array[(String, (Int, BigDecimal))] = {
    rdd.filter(message => {
      message.contains("TSSCJ")
    })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
      .map(message => {
        JsonUtil.parseKakfaRecordToTsscjRecord(message)
      })
      .filter(record => record != null && !"".equalsIgnoreCase(record.khh) && !"".equalsIgnoreCase(record.cjbh))
      .map(record => (record.channel, record))
      .aggregateByKey((0, BigDecimal(0)))(
        (acc, record) => {
          val bz = record.bz.toUpperCase
          val khh = record.khh
          val yyb = record.yyb
          val tempKhh = khh.substring(4).toInt
          val channel = record.channel
          // 更新客户号到Redis
          val jedis = RedisUtil.getConn()
          jedis.setbit(s"${TsscjOperator.TSSCJ_KHH_PREFIX}${yyb}_$channel", tempKhh, true)
          RedisUtil.closeConn(jedis)
          val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
          val cjje = record.cjje * exchange
          (acc._1 + 1, acc._2 + cjje)
        },
        (p1, p2) => {
          (p1._1 + p2._1, p1._2 + p2._2)
        }
      )
      .collect()
  }
}

object TsscjOperator {
  val TSSCJ_KHH_PREFIX: String = "trade_monitor_tsscj_khh_"

  def apply(rdd: RDD[String],
            exchangeMapBC: Broadcast[Map[String, BigDecimal]]
           ): TsscjOperator = new TsscjOperator(rdd, exchangeMapBC)
}


