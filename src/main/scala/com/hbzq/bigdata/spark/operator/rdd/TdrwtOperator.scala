package com.hbzq.bigdata.spark.operator.rdd

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain.TdrwtRecord
import com.hbzq.bigdata.spark.utils.{DateUtil, HBaseUtil, JsonUtil, RedisUtil}
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
class TdrwtOperator(var rdd: RDD[ConsumerRecord[String, String]],
                    var exchangeMapBC: Broadcast[Map[String, BigDecimal]]) extends RddOperator {

  override def compute(): Array[((String, String), (Int, BigDecimal))] = {
    val inputRdd = rdd
      .filter(message => {
        message.topic().equalsIgnoreCase(ConfigurationManager.getProperty(Constants.KAFKA_TOPIC_TWDWT_NAME))
      })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
      .map(message => {
        JsonUtil.parseKakfaRecordToTdrwtRecord(message.value())
      })
      .filter(record => record != null &&
        !"0".equalsIgnoreCase(record.wth)
      )
      .persist(StorageLevel.MEMORY_ONLY_SER)
    // INSERT消息批量插入Hbase
    inputRdd
      .filter(record => record.op.equalsIgnoreCase("INSERT"))
      .foreachPartition(
        records => {
          val puts = records.toList
            .map(record =>
              HBaseUtil.parseTdrwtToPut(record, ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS))
            )
          HBaseUtil.BatchMultiColMessageToHBase(ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE), puts)
        }
      )
    // 处理update消息
    inputRdd
      .map(record => (record.wth, record))
      .groupByKey()
      .mapPartitions[TdrwtRecord](records => {
      import scala.collection.mutable.Map
      val tdrwtInfos = Map[String, TdrwtRecord]()
      var resList: List[TdrwtRecord] = List()
      for ((wth, tdrwtRecords) <- records) {
        tdrwtRecords.filter(_.op.equalsIgnoreCase("INSERT")).foreach(record => {
          tdrwtInfos.put(wth, record)
          TdrwtOperator.logger.info(
            s"""
               |Add Tdrwt detail to Map
               |$wth
               |
              """.stripMargin)
        })
        for (tdrwtRecord <- tdrwtRecords) {
          // 从同一批消息获取
          if (tdrwtRecord.op.equalsIgnoreCase("UPDATE")) {
            val tdrwtDetail = tdrwtInfos.getOrElse(wth, null)
            if (tdrwtDetail != null) {
              TdrwtOperator.logger.info(
                s"""
                   |Get Tdrwt detail from Map
                   |$wth
                   |
              """.stripMargin)
              tdrwtRecord.khh = tdrwtDetail.khh
              tdrwtRecord.wtjg = tdrwtDetail.wtjg
              tdrwtRecord.channel = tdrwtDetail.channel
              tdrwtRecord.yyb = tdrwtDetail.yyb
              tdrwtRecord.wtsl = tdrwtDetail.wtsl
              tdrwtRecord.bz = tdrwtDetail.bz
              resList ::= tdrwtRecord
            } else {
              // HBase中获取
              val data = HBaseUtil.getMessageStrFromHBaseByAllCol(
                ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
                HBaseUtil.getRowKeyFromInteger(wth.toInt),
                ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS)
              )

              TdrwtOperator.logger.info(
                s"""
                   |Get Tdrwt detail from HBase
                   |$wth
                   |
                 """.stripMargin)

              tdrwtRecord.khh = data.get("KHH").getOrElse("")
              tdrwtRecord.yyb = data.get("YYB").getOrElse("")
              tdrwtRecord.bz = data.get("BZ").getOrElse("")
              tdrwtRecord.wtsl = data.get("WTSL").getOrElse("0").toInt
              tdrwtRecord.wtjg = BigDecimal(data.get("WTJG").getOrElse("0"))
              tdrwtRecord.channel = data.get("CHANNEL").getOrElse("qt")
              resList ::= tdrwtRecord
            }
          }
        }
      }
      resList.iterator
    })
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

  val logger = Logger.getLogger(TdrwtOperator.getClass)

  val TDRWT_KHH_PREFIX: String = "trade_monitor_tdrwt_khh_"

  def apply(rdd: RDD[ConsumerRecord[String, String]],
            exchangeMapBC: Broadcast[Map[String, BigDecimal]]
           ): TdrwtOperator = new TdrwtOperator(rdd, exchangeMapBC)

}
