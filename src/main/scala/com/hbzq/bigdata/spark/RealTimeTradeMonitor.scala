package com.hbzq.bigdata.spark

import java.time.LocalDateTime

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain._
import com.hbzq.bigdata.spark.operator.rdd._
import com.hbzq.bigdata.spark.operator.runnable.{FlushRedisToMysqlTask, RedisDelKeyTask}
import com.hbzq.bigdata.spark.utils._
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis
import scalikejdbc.{DB, SQL}

import scala.collection.mutable
import scala.util.Random


/**
  * describe:
  * create on 2020/05/26
  *
  * 实时交易监控程序入口
  *
  * @author hqbhoho
  * @version [v1.0]
  *
  */
object RealTimeTradeMonitor {
  private[this] val logger = Logger.getLogger(RealTimeTradeMonitor.getClass)

  def main(args: Array[String]): Unit = {
    // 获取Kafka配置
    var (topics, kafkaParams) = ConfigurationManager.getKafkaConfig()
    // 启动  先删除Redis  key
    RedisDelKeyTask().run()
    var ssc: StreamingContext = startApp(topics, kafkaParams)
    if (!ssc.awaitTerminationOrTimeout(DateUtil.getDurationTime())) {
      logger.warn(
        s"""
           |====================
           |Begin to stop APP
           |${LocalDateTime.now()}
           |====================
        """.stripMargin)
      ssc.stop(true, true)
    }
  }

  /**
    * 启动Sparkr任务
    *
    * @param topics
    * @param kafkaParams
    * @return
    */
  private def startApp(topics: Array[String], kafkaParams: Map[String, Object]): StreamingContext = {
    val now = DateUtil.getFormatNowDate()
    // 获取spark运行时环境
    var (sparkContext, spark, ssc) = SparkUtil.getSparkStreamingRunTime(ConfigurationManager.getInt(Constants.SPARK_BATCH_DURATION))
    // 获取汇率表
    val exchangeMap = SparkUtil.getExchangeRateFromHive(spark)
    //    val exchangeMap:Map[String, BigDecimal] =Map()
    // 广播变量
    val exchangeMapBC = sparkContext.broadcast(exchangeMap)
    // 获取输入流
    val inputStream = SparkUtil.getInputStreamFromKafkaByMysqlOffset(ssc, topics, kafkaParams)
    inputStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 提前过滤
      val inputRdd = rdd
        .filter(message => StringUtils.isNotEmpty(message.value()))
        .repartition(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM))
        .map(record => {
          record.topic().toUpperCase match {
            case "CIF_TKHXX" => {
              val tkhxxRecord = JsonUtil.parseKakfaRecordToTkhxxRecord(record.value())
              if (tkhxxRecord != null &&
                !"".equalsIgnoreCase(tkhxxRecord.khh) &&
                tkhxxRecord.khrq == DateUtil.getFormatNowDate() &&
                !TkhxxOperator.QT.equalsIgnoreCase(tkhxxRecord.jgbz)) {
                (Random.nextInt(10000).toString, tkhxxRecord)
              } else {
                ("", NullRecord())
              }
            }
            case "SECURITIES_TSSCJ" => {
              val tsscjRecord = JsonUtil.parseKakfaRecordToTsscjRecord(record.value())
              if (tsscjRecord != null && !"".equalsIgnoreCase(tsscjRecord.khh) && !"0".equalsIgnoreCase(tsscjRecord.wth) &&
                !"".equalsIgnoreCase(tsscjRecord.cjbh) && "O".equalsIgnoreCase(tsscjRecord.cxbz)) {
                (tsscjRecord.wth, tsscjRecord)
              } else {
                ("", NullRecord())
              }
            }
            case "ACCOUNT_TDRZJMX" => {
              val tdrzjmxRecord = JsonUtil.parseKakfaRecordToTdrzjmxRecord(record.value())
              if (tdrzjmxRecord != null &&
                !"".equalsIgnoreCase(tdrzjmxRecord.khh) &&
                !"".equalsIgnoreCase(tdrzjmxRecord.lsh)) {
                (Random.nextInt(10000).toString, tdrzjmxRecord)
              } else {
                ("", NullRecord())
              }

            }
            case "SECURITIES_TDRWT" => {
              val tdrwtRecord = JsonUtil.parseKakfaRecordToTdrwtRecord(record.value())
              if (tdrwtRecord != null &&
                !"0".equalsIgnoreCase(tdrwtRecord.wth)) {
                (tdrwtRecord.wth, tdrwtRecord)
              } else {
                ("", NullRecord())
              }
            }
          }
        })
        .filter(record => StringUtils.isNotEmpty(record._1))
        .groupByKey(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 4)

      val resultMap: Array[mutable.Map[String, Any]] = inputRdd.mapPartitions(records => {

        import scala.collection.mutable.Map

        var res: Map[String, Any] = Map()
        val tdrwtInsertRecords: Map[String, TdrwtRecord] = Map()
        val tdrwtUpdateRecords: Map[String, TdrwtRecord] = Map()
        val tsscjRecords: Map[String, TsscjRecord] = Map()
        val tdrwt: Map[(String, String), (Int, BigDecimal)] = Map()
        val tkhxx: Map[String, Int] = Map()
        val tdrzjmx: Map[String, BigDecimal] = Map()
        val tsscj: Map[(String, String), (Int, BigDecimal, BigDecimal)] = Map()

        // 首先遍历一遍  针对不同的消息进行处理
        records.foreach(recorditer => {
          recorditer._2.foreach(record => {
            record match {
              // 客户信息表
              case tkhxxRecord: TkhxxRecord => {
                val oldValue = tkhxx.getOrElse(tkhxxRecord.jgbz, 0)
                tkhxx.put(tkhxxRecord.jgbz, oldValue + 1)
              }
              // 实时成交表
              case tsscjRecord: TsscjRecord => {
                tsscjRecords.put(tsscjRecord.wth, tsscjRecord)
              }
              // 资金明细
              case tdrzjmxRecord: TdrzjmxRecord => {
                val bz = tdrzjmxRecord.bz.toUpperCase
                val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
                val opje = tdrzjmxRecord.je * exchange
                val oldValue = tdrzjmx.getOrElse(tdrzjmxRecord.op, BigDecimal(0))
                tdrzjmx.put(tdrzjmxRecord.op, oldValue + opje)
              }
              // 实时委托表
              case tdrwtRecord: TdrwtRecord => {
                tdrwtRecord.op match {
                  case "INSERT" => tdrwtInsertRecords.put(tdrwtRecord.wth, tdrwtRecord)
                  case "UPDATE" => tdrwtUpdateRecords.put(tdrwtRecord.wth, tdrwtRecord)
                }
              }
              /*
              先到但未关联到HBase中的数据的消息，重新将消息发回Kafka,并增加消息的版本信息,
              默认如果被反复处理固定的次数，将不再关联,并按照未关联的默认值输出
               */
              case _ => {
                // TODO 后续增加未关联Tsscj记录
              }
            }
          })
        })

        // 分别处理 tdrwt, tsscj 两张表的记录
        // 批量插入HBase
        val puts = tdrwtInsertRecords.values
          .map(tdrwtRecord =>
            HBaseUtil.parseTdrwtToPut(tdrwtRecord, ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS))
          )
          .toList
        HBaseUtil.BatchMultiColMessageToHBase(ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE), puts)

        var jedis: Jedis = null
        try {
          jedis = RedisUtil.getConn()


          val tdrwtUpdateWths = tdrwtUpdateRecords.keySet
          val tdrwtInsertWths = tdrwtInsertRecords.keySet
          val tsscjWths = tsscjRecords.keySet
          // tdrwt insert 与 update wth交集
          val tdrwtMixWths = tdrwtUpdateWths & tdrwtInsertWths
          // tdrwt  update wth独享集
          val tdrwtOnlyUpdateWths = tdrwtUpdateWths -- tdrwtMixWths
          // tsscj insert 与 update wth交集
          val tsscjMixWths = tsscjWths & tdrwtInsertWths
          // tsscj  update wth独享集
          val tsscjOnlyWths = tsscjWths -- tsscjMixWths
          val onlyWths = tsscjOnlyWths ++ tdrwtOnlyUpdateWths
          // 批量获取Hbase中的  两个独享集里面的所有数据
          val hbaseTdrwtReords: Map[String, Map[String, String]] = HBaseUtil.getRecordsFromHBaseByKeys(
            ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
            ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS),
            onlyWths)

          var tdrwtInsert = 0
          var tdrwtHbase = 0
          var tsscjInsert = 0
          var tsscjHbase = 0

          // 计算tdrwt相关指标
          tdrwtUpdateRecords.foreach(entry => {
            val wth = entry._1
            val tdrwtRecord = entry._2
            var khh: String = null
            var wtjg: BigDecimal = null
            var channel: String = null
            var yyb: String = null
            var wtsl: Int = 0
            var bz: String = null
            if (tdrwtMixWths.contains(wth)) {
              tdrwtInsert = tdrwtInsert + 1
              val tdrwtDetail = tdrwtInsertRecords.get(wth).get
              khh = tdrwtDetail.khh
              wtjg = tdrwtDetail.wtjg
              channel = tdrwtDetail.channel
              yyb = tdrwtDetail.yyb
              wtsl = tdrwtDetail.wtsl
              bz = tdrwtDetail.bz
            } else {
              tdrwtHbase = tdrwtHbase + 1
              // HBase中获取
              val data = hbaseTdrwtReords.get(wth).get
              khh = data.get("KHH").getOrElse("")
              yyb = data.get("YYB").getOrElse("")
              bz = data.get("BZ").getOrElse("")
              wtsl = data.get("WTSL").getOrElse("0").toInt
              wtjg = BigDecimal(data.get("WTJG").getOrElse("0"))
              channel = data.get("CHANNEL").getOrElse("qt")
            }
            if (!"".equalsIgnoreCase(khh) && khh.length > 4) {
              val tempKhh = khh.substring(4).toInt
              // 更新Redis
              jedis.setbit(s"${TdrwtOperator.TDRWT_KHH_PREFIX}${DateUtil.getFormatNowDate()}_${yyb}_$channel", tempKhh, true)
            } else {
              logger.error(
                s"""
                   |===================
                   |TdrwtUpdate Operator khh is invaild....
                   |$khh
                   |===================
                 """.stripMargin)
            }
            val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
            val wtje = wtsl * wtjg * exchange
            val oldValue = tdrwt.getOrElse((yyb, channel), (0, BigDecimal(0)))
            tdrwt.put((yyb, channel), (oldValue._1 + 1, oldValue._2 + wtje))
          })
          // 计算Tsscj
          tsscjRecords.foreach(entry => {
            val wth = entry._1
            val tsscjRecord = entry._2
            var channel: String = tsscjRecord.channel
            if (tsscjMixWths.contains(wth)) {
              tsscjInsert = tsscjInsert + 1
              val tdrwtDetail = tdrwtInsertRecords.get(wth).get
              channel = tdrwtDetail.channel
            } else {
              tsscjHbase = tsscjHbase + 1
              // HBase中获取
              val data = hbaseTdrwtReords.get(wth).get
              channel = data.get("CHANNEL").getOrElse("undefine")
            }
            val bz = tsscjRecord.bz.toUpperCase
            val khh = tsscjRecord.khh
            val yyb = tsscjRecord.yyb
            val jyj = tsscjRecord.s1
            // 更新Redis
            if (!"".equalsIgnoreCase(khh) && khh.length > 4) {
              val tempKhh = khh.substring(4).toInt
              jedis.setbit(s"${TsscjOperator.TSSCJ_KHH_PREFIX}${DateUtil.getFormatNowDate()}_${yyb}_$channel", tempKhh, true)
            } else {
              logger.error(
                s"""
                   |===================
                   |TSSCJ Operator khh is invaild....
                   |$khh
                   |===================
                 """.stripMargin)
            }

            val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
            val cjje = tsscjRecord.cjje * exchange
            val oldValue = tsscj.getOrElse((yyb, channel), (0, BigDecimal(0), BigDecimal(0)))
            tsscj.put((yyb, channel), (oldValue._1 + 1, oldValue._2 + cjje, oldValue._3 + jyj))
          })

          logger.info(
            s"""
               |===================
               |Data get from
               |tdrwtInsert : $tdrwtInsert
               |tdrwtHbase  : $tdrwtHbase
               |tsscjInsert : $tsscjInsert
               |tsscjHbase  : $tsscjHbase
               |===================
             """.stripMargin)

          hbaseTdrwtReords.clear()
        } catch {
          case ex: Exception => ex.printStackTrace()
        } finally {
          RedisUtil.closeConn(jedis)
        }

        tdrwtInsertRecords.clear()
        tdrwtUpdateRecords.clear()
        tsscjRecords.clear()

        res += ("tdrwt" -> tdrwt)
        res += ("tsscj" -> tsscj)
        res += ("tdrzjmx" -> tdrzjmx)
        res += ("tkhxx" -> tkhxx)
        List(res).iterator
      }).collect()

      import scala.collection.mutable.Map

      // 计算委托相关的业务  委托笔数  委托金额  (yyb , channel)
      val tdrwt: Map[(String, String), (Int, BigDecimal)] = Map()
      // 新增客户数
      val tkhxx: Map[String, Int] = Map()
      // 计算客户转入转出
      val tdrzjmx: Map[String, BigDecimal] = Map()
      // 计算成交相关的业务 成交笔数  成交金额  佣金   (yyb , channel)
      val tsscj: Map[(String, String), (Int, BigDecimal, BigDecimal)] = Map()

      resultMap.foreach(resMap => {
        resMap.foreach(entry => {
          val resKey = entry._1
          val resValue = entry._2
          resKey match {
            case "tdrwt" => {
              resValue.asInstanceOf[Map[(String, String), (Int, BigDecimal)]].foreach(entry => {
                val key = entry._1
                val value = entry._2
                val oleValue = tdrwt.getOrElse(key, (0, BigDecimal(0)))
                tdrwt.put(key, (oleValue._1 + value._1, oleValue._2 + value._2))
              })
            }
            case "tsscj" => {
              resValue.asInstanceOf[Map[(String, String), (Int, BigDecimal, BigDecimal)]].foreach(entry => {
                val key = entry._1
                val value = entry._2
                val oleValue = tsscj.getOrElse(key, (0, BigDecimal(0), BigDecimal(0)))
                tsscj.put(key, (oleValue._1 + value._1, oleValue._2 + value._2, oleValue._3 + value._3))
              })
            }
            case "tdrzjmx" => {
              resValue.asInstanceOf[Map[String, BigDecimal]].foreach(entry => {
                val key = entry._1
                val value = entry._2
                val oleValue = tdrzjmx.getOrElse(key, BigDecimal(0))
                tdrzjmx.put(key, oleValue + value)
              })
            }
            case "tkhxx" => {
              resValue.asInstanceOf[Map[String, Int]].foreach(entry => {
                val key = entry._1
                val value = entry._2
                val oleValue = tkhxx.getOrElse(key, 0)
                tkhxx.put(key, oleValue + value)
              })
            }
          }
        })
      })

      // 事物更新offset 及结果到Mysql
      DB.localTx(implicit session => {
        val timestamp = DateUtil.getNowTimestamp()
        var nowTradeStates: List[List[Any]] = List()
        if (!tdrwt.isEmpty || !tsscj.isEmpty) {
          val keys = tdrwt.keySet ++ tsscj.keySet;
          for ((yyb, channel) <- keys) {
            var (now_wt_tx_count, now_wt_tx_sum) = tdrwt.getOrElse((yyb, channel), (0, BigDecimal(0)))
            var (cj_tx_count, cj_tx_sum, jyj) = tsscj.getOrElse((yyb, channel), (0, BigDecimal(0), BigDecimal(0)))
            //            val _channel = channels.get(channel).get
            if (now_wt_tx_count != 0 || cj_tx_count != 0) {
              nowTradeStates ::= (channel :: yyb :: now_wt_tx_count :: now_wt_tx_sum :: cj_tx_count :: cj_tx_sum :: jyj :: timestamp :: now :: channel :: yyb :: now_wt_tx_count :: now_wt_tx_sum :: cj_tx_count :: cj_tx_sum :: jyj :: timestamp :: now :: Nil)
            }
          }
          if (!nowTradeStates.isEmpty) {
            nowTradeStates.foreach(tradeState => {
              SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_NOW_TRADE_STATE_SQL))
                .bind(tradeState: _*).update().apply()
            })
          }
        }
        // other_state
        val new_jg_count = tkhxx.getOrElse(TkhxxOperator.GR, 0)
        val new_gr_count = tkhxx.getOrElse(TkhxxOperator.JG, 0)
        val zr_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZR, BigDecimal(0))
        val zc_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZC, BigDecimal(0))
        if (new_jg_count != 0 || new_gr_count != 0 || zr_sum != 0 || zc_sum != 0) {
          var otherStates: List[List[Any]] = List()
          otherStates ::= (new_jg_count :: new_gr_count :: zr_sum :: zc_sum :: timestamp :: now :: new_jg_count :: new_gr_count :: zr_sum :: zc_sum :: timestamp :: now :: Nil)
          otherStates.foreach(otherState => {
            SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_OTHER_STATE_SQL))
              .bind(otherState: _*).update().apply()
          })
        }
        // Kafka Offset
        var kafkaOffsets: List[List[Any]] = List()
        offsetRanges.foreach(offsetRange => {
          SQL(ConfigurationManager.getProperty(Constants.KAFKA_MYSQL_INSERT_OFFSET_SQL))
            .bind(offsetRange.topic, ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID)
              , offsetRange.partition, offsetRange.untilOffset).update().apply()
        })
      })
      // 刷新 Redis 统计数据到Msyql
      FlushRedisToMysqlTask().run()
    })
    ssc.start()
    ssc
  }
}
