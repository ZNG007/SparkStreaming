package com.hbzq.bigdata.spark.operator.rdd

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.utils.{DateUtil, JsonUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Map, Set}

/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
class TjgmxlsOperator(var rdd: RDD[String]) extends RddOperator {

  override def compute(): Array[(String, BigDecimal)] = {
    rdd.filter(message => {
      message.contains("TJGMXLS")
    })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
      .map(message => {
        JsonUtil.parseKakfaRecordToTjgmxlsRecord(message)
      })
      .filter(record => {
        record != null &&
          !"".equalsIgnoreCase(record.khh) &&
          !"".equalsIgnoreCase(record.lsh) &&
          !"".equals(record.yyb)
      })
      .map(record => (record.yyb, record))
      .aggregateByKey(BigDecimal(0))(
        (acc, record) => {
          val jyj = record.s1 - record.s11 - record.s12 - record.s13
          acc + jyj
        },
        (p1, p2) => {
          p1 + p2
        }
      )
      .collect()
  }
}

object TjgmxlsOperator {
  def apply(rdd: RDD[String]): TjgmxlsOperator = new TjgmxlsOperator(rdd)
}
