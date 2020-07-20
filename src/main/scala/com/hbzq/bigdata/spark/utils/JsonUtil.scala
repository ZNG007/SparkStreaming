package com.hbzq.bigdata.spark.utils

import java.io.File

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain._
import com.hbzq.bigdata.spark.operator.rdd.{TdrzjmxOperator, TkhxxOperator}
import com.owlike.genson.defaultGenson._
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger

import scala.reflect.runtime.universe._

/**
  * describe:
  * create on 2020/05/27
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */


object JsonUtil {

  private[this] val logger = Logger.getLogger(JsonUtil.getClass)


  /**
    * 读取Json文件获取
    *
    * @param fileName 文件路径
    * @return
    */
  def parseRuleFile(fileName: String): Map[String, List[Map[String, List[String]]]] = {
    try {
      fromJson[Map[String, List[Map[String, List[String]]]]](FileUtils.readFileToString(new File(fileName), "UTF-8"))
    } catch {
      case ex: Exception => {
        throw new RuntimeException("rule json format is not vaild,please check....")
      }
    }
  }

  /**
    * 转换记录 --> TDRWT
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTdrwtRecord(message: String): TdrwtRecord = {
    val (op, after, before) = parseAllJsonStringToMap(message)
    if (after.isEmpty) {
      return null
    }
    op.toUpperCase match {
      // INSERT消息  将channel 等消息插入 HBase
      case "INSERT" => {
        val khh = after.get("KHH").getOrElse("")
        val wth = after.get("WTH").getOrElse("0")
        val yyb = after.get("YYB").getOrElse("")
        val wtfs = after.get("WTFS").getOrElse("")
        val wtgy = after.get("WTGY").getOrElse("")
        val bz = after.get("BZ").getOrElse("")
        val wtsl = after.get("WTSL").getOrElse("0")
        val wtjg = BigDecimal(after.get("WTJG").getOrElse("0"))
        val tdrwtRecord = TdrwtRecord(
          khh,
          wth,
          yyb,
          wtfs,
          wtgy,
          bz,
          wtsl.toInt,
          wtjg
        )
        val channel = tdrwtRecord.matchClassify(
          RuleVaildUtil.getClassifyListByRuleName(Constants.RULE_TX_CHANNEL_CLASSIFY),
          typeOf[TdrwtRecord])

        val data = Map(
          "KHH" -> khh, "WTH" -> wth, "YYB" -> yyb,
          "BZ" -> bz, "CHANNEL" -> channel,
          "WTSL" -> wtsl, "WTJG" -> wtjg
        )
        // 将明细数据插入HBase
        HBaseUtil.insertMultiColMessageToHBase(
          ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
          HBaseUtil.getRowKeyFromInteger(wth.toInt),
          ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS),
          data
        )
        null
      }
      // UPDATE消息
      case "UPDATE" => {

        val before_jgsm = before.get("JGSM").getOrElse("").trim
        val after_jgsm = after.get("JGSM").getOrElse("").trim
        val wth = before.get("WTH").getOrElse("0").toInt

        val before_jgsm_list = List("待申报", "申报中")
        if (after_jgsm.equalsIgnoreCase("已申报") && before_jgsm_list.contains(before_jgsm)) {
          // 从HBase中获取数据
          val data = HBaseUtil.getMessageStrFromHBaseByAllCol(
            ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
            HBaseUtil.getRowKeyFromInteger(wth.toInt),
            ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS)
          )
          TdrwtRecord(
            data.get("KHH").getOrElse(""),
            data.get("WTH").getOrElse("0"),
            data.get("YYB").getOrElse(""),
            "",
            "",
            data.get("BZ").getOrElse(""),
            data.get("WTSL").getOrElse("0").toInt,
            BigDecimal(data.get("WTJG").getOrElse("0")),
            data.get("CHANNEL").getOrElse("qt")
          )
        } else {
          null
        }
      }
      case _ => {
        null
      }
    }
  }

  /**
    * 转换记录 --> TSSCJ
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTsscjRecord(message: String): TsscjRecord = {
    val (op, after) = parseAfterJsonStringToMap(message)
    if (!"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
      return null
    }
    // TSSCJ "KHH","WTH","YYB","WTFS","WTGY","BZ","CJJE"
    val tsscjRecord = TsscjRecord(
      after.get("KHH").getOrElse(""),
      after.get("CJBH").getOrElse(""),
      after.get("YYB").getOrElse(""),
      after.get("BZ").getOrElse(""),
      BigDecimal(after.get("CJJE").getOrElse("0")),
      BigDecimal(after.get("S1").getOrElse("0")),
      after.get("CXBZ").getOrElse(""),
      after.get("WTH").getOrElse("0")
    )
    tsscjRecord
  }

  /**
    * 转换记录 --> TKHXX
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTkhxxRecord(message: String): TkhxxRecord = {
    val (op, after) = parseAfterJsonStringToMap(message)
    if (!"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
      return null
    }
    val jgbz = after.get("JGBZ").getOrElse("-1").toInt
    jgbz match {
      case 0 => TkhxxRecord(
        after.get("KHH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        TkhxxOperator.GR
      )
      case 1 => TkhxxRecord(
        after.get("KHH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        TkhxxOperator.JG
      )
      case _ => TkhxxRecord(
        after.get("KHH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        TkhxxOperator.QT
      )
    }
  }

  /**
    * 转换记录 --> TDRZJMX
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTdrzjmxRecord(message: String): TdrzjmxRecord = {
    val (op, after) = parseAfterJsonStringToMap(message)
    if (!"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
      return null
    }

    val srje = BigDecimal(after.get("SRJE").getOrElse("0"))
    val fcje = BigDecimal(after.get("FCJE").getOrElse("0"))

    val ywkm = after.get("YWKM").getOrElse("")

    if (TdrzjmxOperator.TDRZJMX_SR_YWKM_LIST.contains(ywkm)) {
      TdrzjmxRecord(
        after.get("KHH").getOrElse(""),
        after.get("LSH").getOrElse(""),
        after.get("YWKM").getOrElse(""),
        after.get("BZ").getOrElse(""),
        TdrzjmxOperator.ZJZR,
        srje
      )
    } else if (TdrzjmxOperator.TDRZJMX_FC_YWKM_LIST.contains(ywkm)) {
      TdrzjmxRecord(
        after.get("KHH").getOrElse(""),
        after.get("LSH").getOrElse(""),
        after.get("YWKM").getOrElse(""),
        after.get("BZ").getOrElse(""),
        TdrzjmxOperator.ZJZC,
        fcje
      )
    } else {
      null
    }
  }

  /**
    * 解析KafkaRecord 获取after信息
    *
    * @param message
    * @return
    */
  def getAfterInfoFromKafkaRecord(message: Map[String, Any]): (String, Map[String, String]) = {

    val after = message.get("after").getOrElse(Map()).asInstanceOf[Map[String, String]]
    var op = message.get("optype").getOrElse("").asInstanceOf[String]
    (op, after)
  }

  /**
    * 解析KafkaRecord 获取所有信息
    *
    * @param message
    * @return
    */
  def getAllInfoFromKafkaRecord(message: Map[String, Any]): (String, Map[String, String], Map[String, String]) = {

    val after = message.get("after").getOrElse(Map()).asInstanceOf[Map[String, String]]
    val before = message.get("before").getOrElse(Map()).asInstanceOf[Map[String, String]]
    var op = message.get("optype").getOrElse("").asInstanceOf[String]
    (op, after, before)
  }

  /**
    *
    * 将 json字符串转Map对象
    *
    * @param message
    */
  def parseAfterJsonStringToMap(message: String): (String, Map[String, String]) = {
    try {
      val parseMap = fromJson[Map[String, Any]](message)
      getAfterInfoFromKafkaRecord(parseMap)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.warn(
          s"""
             |====WARN====
             |kafka record json format is not vaild,please check
             |kafka record :
             |$message
             |============
          """.stripMargin)
        ("", Map())
      }
    }
  }

  /**
    *
    * 将 json字符串转Map对象
    *
    * @param message
    */
  def parseAllJsonStringToMap(message: String): (String, Map[String, String], Map[String, String]) = {
    try {
      val parseMap = fromJson[Map[String, Any]](message)
      getAllInfoFromKafkaRecord(parseMap)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.warn(
          s"""
             |====WARN====
             |kafka record json format is not vaild,please check
             |kafka record :
             |$message
             |============
          """.stripMargin)
        ("", Map(), Map())
      }
    }
  }
}
