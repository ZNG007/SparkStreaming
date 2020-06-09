package com.hbzq.bigdata.spark.operator.rdd

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.utils.{DateUtil, JsonUtil, RedisUtil}
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
class TkhxxOperator(var rdd: RDD[String]) extends RddOperator {


  override def compute(): Array[(String, Int)] = {

    rdd.filter(message => {
      message.contains("TKHXX")
    })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
      .map(message => {
        JsonUtil.parseKakfaRecordToTkhxxRecord(message)
      })
      .filter(record => {
        record != null &&
          !"".equalsIgnoreCase(record.khh) &&
          record.khrq == DateUtil.getFormatNowDate() &&
          !"".equalsIgnoreCase(record.jgbz)
      })
      .map(record => (record.jgbz, 1))
      .aggregateByKey(0)(
        (acc, record) => {
          acc + record
        },
        (p1, p2) => {
          p1 + p2
        }
      )
      .collect()
  }
}

object TkhxxOperator {

  val JG: String = "jg"
  val GR: String = "gr"
  val QT: String = ""

  def apply(rdd: RDD[String]): TkhxxOperator = new TkhxxOperator(rdd)
}
