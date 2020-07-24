package com.hbzq.bigdata.spark.operator.record

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain._
import com.hbzq.bigdata.spark.operator.rdd.{TdrwtOperator, TkhxxOperator, TsscjOperator}
import com.hbzq.bigdata.spark.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.util.Random

/**
  * describe:
  * create on 2020/07/24
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object BaseRecordOperator {
  private[this] val logger = Logger.getLogger(BaseRecordOperator.getClass)

  /**
    * 将Kafk中的消息转化成BaseRecord
    *
    * @param records
    * @return
    */
  def parsePartitionKafkaRecordsToBaseRecord(records: Iterator[ConsumerRecord[String, String]]): Iterator[(String, BaseRecord)] = {

    var res: List[(String, BaseRecord)] = List()
    records.foreach(record => {
      record.topic().toUpperCase match {
        // 新开户
        case "CIF_TKHXX" => {
          val tkhxxRecord = JsonUtilV2.parseKakfaRecordToTkhxxRecord(record.value())
          if (tkhxxRecord != null &&
            !"".equalsIgnoreCase(tkhxxRecord.khh) &&
            tkhxxRecord.khrq == DateUtil.getFormatNowDate() &&
            !TkhxxOperator.QT.equalsIgnoreCase(tkhxxRecord.jgbz)) {
            res ::= (Random.nextInt(10000).toString, tkhxxRecord)
          }
        }
        // 实时成交
        case "SECURITIES_TSSCJ" => {
          val tsscjRecord = JsonUtilV2.parseKakfaRecordToTsscjRecord(record.value())
          if (tsscjRecord != null &&
            !"".equalsIgnoreCase(tsscjRecord.khh) &&
            !"0".equalsIgnoreCase(tsscjRecord.wth) &&
            !"".equalsIgnoreCase(tsscjRecord.cjbh) &&
            "O".equalsIgnoreCase(tsscjRecord.cxbz)) {
            res ::= (tsscjRecord.wth, tsscjRecord)
          }
        }
        // 当日资金明细
        case "ACCOUNT_TDRZJMX" => {
          val tdrzjmxRecord = JsonUtilV2.parseKakfaRecordToTdrzjmxRecord(record.value())
          if (tdrzjmxRecord != null &&
            !"".equalsIgnoreCase(tdrzjmxRecord.khh) &&
            !"".equalsIgnoreCase(tdrzjmxRecord.lsh)) {
            res ::= (Random.nextInt(10000).toString, tdrzjmxRecord)
          }

        }
        // 当日委托
        case "SECURITIES_TDRWT" => {
          val tdrwtRecord = JsonUtilV2.parseKakfaRecordToTdrwtRecord(record.value())
          if (tdrwtRecord != null &&
            !"0".equalsIgnoreCase(tdrwtRecord.wth)) {
            res ::= (tdrwtRecord.wth, tdrwtRecord)
          }
        }

        /**
          * 先到但未关联到HBase中的数据的消息，重新将消息发回Kafka,并增加消息的版本信息,
          * 默认如果被反复处理固定的次数，将不再关联,并按照未关联的默认值输出
          * 目前针对的是Tsscj未关联到的消息
          */
        case "TRADE_MONITOR_SLOW" => {
          val slowMessageRecord = JsonUtilV2.parseKakfaRecordToSlowMessageRecord(record.value())
          slowMessageRecord.originTopic match {
            case "SECURITIES_TSSCJ" => {
              val tsscjRecord = slowMessageRecord.baseRecord.asInstanceOf[TsscjRecord]
              if (tsscjRecord != null &&
                !"".equalsIgnoreCase(tsscjRecord.khh)
                && !"0".equalsIgnoreCase(tsscjRecord.wth) &&
                !"".equalsIgnoreCase(tsscjRecord.cjbh) &&
                "O".equalsIgnoreCase(tsscjRecord.cxbz)) {
                res ::= (tsscjRecord.wth, tsscjRecord)
              }
            }
            case _ =>
          }
        }
      }
    })
    res.iterator
  }


  /**
    * 分区数据聚合操作
    *
    * @param records
    * @param exchangeMapBC
    * @param kafkaProducerBC
    * @return
    */
  def aggregatePartitionBaseRecords(records: Iterator[(String, Iterable[BaseRecord])],
                                    exchangeMapBC: Broadcast[Map[String, BigDecimal]],
                                    kafkaProducerBC: Broadcast[KafkaSink[String, String]]): Iterator[mutable.Map[String, Any]] = {
    import scala.collection.mutable.Map
    // 结果集
    var res: mutable.Map[String, Any] = Map()
    // 委托插入及更新集合
    val tdrwtInsertRecords: mutable.Map[String, TdrwtRecord] = Map()
    val tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord] = Map()
    // 成交集合
    val tsscjRecords: mutable.Map[String, TsscjRecord] = Map()
    // 未关联集合
    var slowMessageList: List[SlowMessageRecord] = List()
    // 成交，委托，新开户，资金 结果集
    val tdrwt: mutable.Map[(String, String), (Int, BigDecimal)] = Map()
    val tkhxx: mutable.Map[String, Int] = Map()
    val tdrzjmx: mutable.Map[String, BigDecimal] = Map()
    val tsscj: mutable.Map[(String, String), (Int, BigDecimal, BigDecimal)] = Map()
    // 首先遍历一遍  针对不同的消息进行处理
    classifyAndComputeBaseRecord(records, tdrwtInsertRecords, tdrwtUpdateRecords, tsscjRecords, tkhxx, tdrzjmx, exchangeMapBC)
    //  TDRWT Insert 批量插入HBase
    putTdrwtRecordsToHBase(tdrwtInsertRecords)
    var jedis: Jedis = null
    try {
      jedis = RedisUtil.getConn()
      val (tdrwtMixWths, tsscjMixWths, hbaseTdrwtReords) = getAllTdrwtFromHBaseOrPartition(tdrwtInsertRecords, tdrwtUpdateRecords, tsscjRecords)
      // 计算tdrwt相关指标
      computeTdrwtUpdateRecord(tdrwtUpdateRecords, tdrwtInsertRecords, tdrwtMixWths, hbaseTdrwtReords, exchangeMapBC, tdrwt, jedis)
      // 计算Tsscj
      slowMessageList ::= computeTsscjRecord(tsscjRecords, tdrwtInsertRecords, tsscjMixWths, hbaseTdrwtReords, exchangeMapBC, tsscj, jedis)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      RedisUtil.closeConn(jedis)
    }
    // 往Kafka 发送未关联消息
    pushMassageToKafka(slowMessageList,kafkaProducerBC)
    res += ("tdrwt" -> tdrwt)
    res += ("tsscj" -> tsscj)
    res += ("tdrzjmx" -> tdrzjmx)
    res += ("tkhxx" -> tkhxx)
    List(res).iterator
  }

  /**
    * 推送消息到Kafka
    *
    * @param slowMessageList
    * @param kafkaProducerBC
    */
  private def pushMassageToKafka(slowMessageList: List[SlowMessageRecord],kafkaProducerBC: Broadcast[KafkaSink[String, String]]) = {
    slowMessageList
      .map(message => JsonUtilV2.parseObjectToJson(message))
      .foreach(message => {
        kafkaProducerBC.value.send(ConfigurationManager.getProperty(Constants.KAFKA_TOPIC_TRADE_MONITOR_SLOW_NAME), message)
      })
  }

  /**
    * 计算 tsscj 指标
    *
    * @param tsscjRecords
    * @param tdrwtInsertRecords
    * @param tsscjMixWths
    * @param hbaseTdrwtReords
    * @param exchangeMapBC
    * @param tsscj
    * @param jedis
    */
  def computeTsscjRecord(tsscjRecords: mutable.Map[String, TsscjRecord],
                         tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                         tsscjMixWths: collection.Set[String],
                         hbaseTdrwtReords: mutable.Map[String, mutable.Map[String, String]],
                         exchangeMapBC: Broadcast[Map[String, BigDecimal]],
                         tsscj: mutable.Map[(String, String), (Int, BigDecimal, BigDecimal)],
                         jedis: Jedis):List[SlowMessageRecord] = {
    var slowMessageList: List[SlowMessageRecord] = List()
    tsscjRecords.foreach(entry => {
      val wth = entry._1
      val tsscjRecord = entry._2
      var channel: String = tsscjRecord.channel
      var flag = true
      if (tsscjMixWths.contains(wth)) {
        val tdrwtDetail = tdrwtInsertRecords.get(wth).get
        channel = tdrwtDetail.channel
      } else {
        // HBase中获取
        if (hbaseTdrwtReords.keySet.contains(wth)) {
          val data = hbaseTdrwtReords.get(wth).get
          channel = data.get("CHANNEL").getOrElse("undefine")
        } else {
          // 关联不到的记录处理

          val threshold = ConfigurationManager.getInt(Constants.TSSCJ_LIFE_CYCLE_THRESHOLD)
          val version = tsscjRecord.version
          flag = version match {
            case version if (version < threshold) => {
              logger.info(s"R1-3 $tsscjRecord  ")
              tsscjRecord.version = version + 1
              val slowMessage = SlowMessageRecord("SECURITIES_TSSCJ", tsscjRecord)
              slowMessageList ::= slowMessage
              false
            }
            case _ => {
              logger.info(s"R1-4 $tsscjRecord  ")
              logger.error(
                s"""
                   |==================
                   |over threshold : ${threshold}
                   |can't get channel from HBase, use defalut value to compute....
                   |$tsscjRecord
                   |==================
                  """.stripMargin)
              true
            }
          }
        }
      }
      if (flag) {
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
      }
    })
    slowMessageList
  }

  /**
    * 计算tdrwt相关指标
    *
    * @param tdrwtUpdateRecords
    * @param tdrwtInsertRecords
    * @param tdrwtMixWths
    * @param hbaseTdrwtReords
    * @param exchangeMapBC
    * @param tdrwt
    * @param jedis
    */
  private def computeTdrwtUpdateRecord(tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord],
                                       tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                                       tdrwtMixWths: collection.Set[String],
                                       hbaseTdrwtReords: mutable.Map[String, mutable.Map[String, String]],
                                       exchangeMapBC: Broadcast[Map[String, BigDecimal]],
                                       tdrwt: mutable.Map[(String, String), (Int, BigDecimal)],
                                       jedis: Jedis) = {
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
        val tdrwtDetail = tdrwtInsertRecords.get(wth).get
        khh = tdrwtDetail.khh
        wtjg = tdrwtDetail.wtjg
        channel = tdrwtDetail.channel
        yyb = tdrwtDetail.yyb
        wtsl = tdrwtDetail.wtsl
        bz = tdrwtDetail.bz
      } else {
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
             |khh: $khh
             |===================
            """.stripMargin)
      }
      val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
      val wtje = wtsl * wtjg * exchange
      val oldValue = tdrwt.getOrElse((yyb, channel), (0, BigDecimal(0)))
      tdrwt.put((yyb, channel), (oldValue._1 + 1, oldValue._2 + wtje))
    })
  }

  /**
    * 从当前批次的Insert Tdrwt记录或者从HBase中关联出所有能关联到的Tdrwt记录，便于后续 Tdrwt Update记录及Tssc记录的处理
    *
    * @param tdrwtInsertRecords
    * @param tdrwtUpdateRecords
    * @param tsscjRecords
    * @return
    */
  private def getAllTdrwtFromHBaseOrPartition(tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                                              tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord],
                                              tsscjRecords: mutable.Map[String, TsscjRecord]) = {
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
    val hbaseTdrwtReords: mutable.Map[String, mutable.Map[String, String]] = HBaseUtil.getRecordsFromHBaseByKeys(
      ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
      ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS),
      onlyWths)
    (tdrwtMixWths, tsscjMixWths, hbaseTdrwtReords)
  }

  /**
    * 分类不同来源的记录，并针对不同的消息记录进行不同的处理
    *
    * @param records
    * @param tdrwtInsertRecords
    * @param tdrwtUpdateRecords
    * @param tsscjRecords
    * @param tkhxx
    * @param tdrzjmx
    * @param exchangeMapBC
    */
  private def classifyAndComputeBaseRecord(records: Iterator[(String, Iterable[BaseRecord])],
                                           tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                                           tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord],
                                           tsscjRecords: mutable.Map[String, TsscjRecord],
                                           tkhxx: mutable.Map[String, Int],
                                           tdrzjmx: mutable.Map[String, BigDecimal],
                                           exchangeMapBC: Broadcast[Map[String, BigDecimal]]) = {
    records.foreach(recorditer => {
      recorditer._2.foreach(record => {
        record match {
          // 新开户
          case tkhxxRecord: TkhxxRecord => {
            val oldValue = tkhxx.getOrElse(tkhxxRecord.jgbz, 0)
            tkhxx.put(tkhxxRecord.jgbz, oldValue + 1)
          }
          // 实时成交
          case tsscjRecord: TsscjRecord => {
            tsscjRecords.put(tsscjRecord.wth, tsscjRecord)
          }
          // 资金转入转出
          case tdrzjmxRecord: TdrzjmxRecord => {
            val bz = tdrzjmxRecord.bz.toUpperCase
            val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
            val opje = tdrzjmxRecord.je * exchange
            val oldValue = tdrzjmx.getOrElse(tdrzjmxRecord.op, BigDecimal(0))
            tdrzjmx.put(tdrzjmxRecord.op, oldValue + opje)
          }
          // 实时委托
          case tdrwtRecord: TdrwtRecord => {
            tdrwtRecord.op match {
              case "INSERT" => tdrwtInsertRecords.put(tdrwtRecord.wth, tdrwtRecord)
              case "UPDATE" => tdrwtUpdateRecords.put(tdrwtRecord.wth, tdrwtRecord)
            }
          }
          case _ =>
        }
      })
    })
  }

  /**
    * 批量插入HBase
    *
    * @param tdrwtInsertRecords
    */
  private def putTdrwtRecordsToHBase(tdrwtInsertRecords: mutable.Map[String, TdrwtRecord]): Unit = {
    val puts = tdrwtInsertRecords.values
      .map(tdrwtRecord =>
        HBaseUtil.parseTdrwtToPut(tdrwtRecord, ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS))
      )
      .toList
    HBaseUtil.BatchMultiColMessageToHBase(ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE), puts)
  }
}
