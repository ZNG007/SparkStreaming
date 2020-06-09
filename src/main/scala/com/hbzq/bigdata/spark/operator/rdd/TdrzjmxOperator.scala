package com.hbzq.bigdata.spark.operator.rdd

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.hbzq.bigdata.spark.RealTimeTradeMonitor
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.utils.{JsonUtil, RedisUtil}
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
class TdrzjmxOperator(var rdd: RDD[String],
                      var exchangeMapBC: Broadcast[Map[String, BigDecimal]]) extends RddOperator {

  override def compute(): Array[(String, BigDecimal)] = {

    rdd.filter(message => {
      message.contains("TDRZJMX")
    })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
      .map(message => {
        JsonUtil.parseKakfaRecordToTdrzjmxRecord(message)
      })
      .filter(record => {
        record != null &&
          !"".equalsIgnoreCase(record.khh) &&
          !"".equalsIgnoreCase(record.lsh)
      })
      .map(record => (record.op, record))
      .aggregateByKey(BigDecimal(0))(
        (acc, record) => {
          val bz = record.bz.toUpperCase
          val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
          val opje = record.je * exchange
          acc + opje
        },
        (p1, p2) => {
          p1 + p2
        }
      )
      .collect()
  }
}

object TdrzjmxOperator {

  val TDRZJMX_SR_YWKM_LIST: List[String] = List("10113")
  val TDRZJMX_FC_YWKM_LIST: List[String] = List("10213")
  val ZJZR: String = "zjzr"
  val ZJZC: String = "zjzc"

  def apply(rdd: RDD[String],
            exchangeMapBC: Broadcast[Map[String, BigDecimal]]
           ): TdrzjmxOperator = new TdrzjmxOperator(rdd, exchangeMapBC)
}
