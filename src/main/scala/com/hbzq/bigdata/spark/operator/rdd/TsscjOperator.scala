package com.hbzq.bigdata.spark.operator.rdd

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain.TsscjRecord
import com.hbzq.bigdata.spark.operator.runnable.FlushRedisToMysqlTask
import com.hbzq.bigdata.spark.utils._
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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

  override def compute(): (Array[(String, (Int, BigDecimal, BigDecimal))], Array[(String, BigDecimal)]) = {
    val tsscjRdd = rdd
      .filter(message => {
        message.contains("TSSCJ")
      })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
      .map(message => {
        JsonUtil.parseKakfaRecordToTsscjRecord(message)
      })
      .filter(record => record != null && !"".equalsIgnoreCase(record.khh) &&
        !"".equalsIgnoreCase(record.cjbh) && "O".equalsIgnoreCase(record.cxbz))
      .map(record => {
        // 从hbase中获取channel
        val channel = getChannelFromHBase(record)
        if (StringUtils.isNotEmpty(channel)) record.channel = channel
        else TsscjOperator.logger.warn(
          s"""
             |===============TSSCJ record match any channel
             |record :
             |$record
             |channel :
             |$channel
             |===============
           """.stripMargin)
        record
      })
      .persist(StorageLevel.MEMORY_ONLY_SER)

    // 按照委托方式   计算成交笔数   成交金额   佣金
    val tsscjTrade: Array[(String, (Int, BigDecimal, BigDecimal))] = tsscjRdd.map(record => (record.channel, record))
      .aggregateByKey((0, BigDecimal(0), BigDecimal(0)))(
        (acc, record) => {
          val bz = record.bz.toUpperCase
          val khh = record.khh
          val tempKhh = khh.substring(4).toInt
          val yyb = record.yyb
          val jyj = record.s1
          val channel = record.channel
          // 更新客户号到Redis
          val jedis = RedisUtil.getConn()
          jedis.setbit(s"${TsscjOperator.TSSCJ_KHH_PREFIX}${yyb}_${DateUtil.getFormatNowDate()}_$channel", tempKhh, true)
          RedisUtil.closeConn(jedis)
          val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
          val cjje = record.cjje * exchange
          (acc._1 + 1, acc._2 + cjje, acc._3 + jyj)
        },
        (p1, p2) => {
          (p1._1 + p2._1, p1._2 + p2._2, p1._3 + p2._3)
        }
      )
      .collect()

    // 按照营业部计算JYJ
    val tsscjJyj: Array[(String, BigDecimal)] = tsscjRdd.map(record => (record.yyb, record))
      .aggregateByKey((BigDecimal(0)))(
        (acc, record) => {
          val jyj = record.s1
          (acc + jyj)
        },
        (p1, p2) => {
          (p1 + p2)
        }
      )
      .collect()
    (tsscjTrade, tsscjJyj)
  }

  /**
    *
    * @param record
    * @return
    */
  private def getChannelFromHBase(record: TsscjRecord) = {
    HBaseUtil.getMessageStrFromHBase(
      ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
      HBaseUtil.getRowKeyFromInteger(record.wth.toInt),
      ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS),
      "channel"
    )
  }
}

object TsscjOperator {

  val logger = Logger.getLogger(TsscjOperator.getClass)

  val TSSCJ_KHH_PREFIX: String = "trade_monitor_tsscj_khh_"

  def apply(rdd: RDD[String],
            exchangeMapBC: Broadcast[Map[String, BigDecimal]]
           ): TsscjOperator = new TsscjOperator(rdd, exchangeMapBC)
}


