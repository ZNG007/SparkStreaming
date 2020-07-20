package com.hbzq.bigdata.spark.operator.rdd

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain.TdrwtRecord
import com.hbzq.bigdata.spark.utils.{DateUtil, HBaseUtil, JsonUtil, RedisUtil}
import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder
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
class TdrwtOperator(var rdd: RDD[String],
                    var exchangeMapBC: Broadcast[Map[String, BigDecimal]]) extends RddOperator {

  override def compute(): Array[((String,String), (Int, BigDecimal))] = {
    rdd
      .filter(message => {
        message.contains("TDRWT")
      })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
      .map(message => {
        JsonUtil.parseKakfaRecordToTdrwtRecord(message)
      })
      .filter(record => record != null &&
        !"".equalsIgnoreCase(record.khh) &&
        !"0".equalsIgnoreCase(record.wth)
      )
      .map(record => {
        ((record.yyb, record.channel), record)
      })
      .aggregateByKey((0, BigDecimal(0)))(
        (acc, record) => {
          val bz = record.bz.trim.toUpperCase
          val khh = record.khh
          val yyb = record.yyb
          val tempKhh = khh.substring(4).toInt
          val channel = record.channel
          // 更新客户号到Redis
          val jedis = RedisUtil.getConn()
          jedis.setbit(s"${TdrwtOperator.TDRWT_KHH_PREFIX}${DateUtil.getFormatNowDate()}_${yyb}_$channel", tempKhh, true)
          RedisUtil.closeConn(jedis)
          val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
          val wtje = record.wtsl * record.wtjg * exchange
          (acc._1 + 1, acc._2 + wtje)
        },
        (p1, p2) => {
          (p1._1 + p2._1, p1._2 + p2._2)
        }
      )
      .collect()
  }

  /**
    *
    * @param record
    */
  private def insertWthMessageToHBase(record: TdrwtRecord) = {
    HBaseUtil.insertSingleColMessageToHBase(
      ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
      HBaseUtil.getRowKeyFromInteger(record.wth.toInt),
      ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS),
      "channel",
      record.channel
    )
  }
}

object TdrwtOperator {

  val TDRWT_KHH_PREFIX: String = "trade_monitor_tdrwt_khh_"

  def apply(rdd: RDD[String],
            exchangeMapBC: Broadcast[Map[String, BigDecimal]]
           ): TdrwtOperator = new TdrwtOperator(rdd, exchangeMapBC)

}
