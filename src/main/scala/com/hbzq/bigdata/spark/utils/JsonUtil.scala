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
    val (tableName, op, owner, after, before) = parseAllJsonStringToMap(message)
    if (!"TDRWT".equalsIgnoreCase(tableName) || !"UPDATE".equalsIgnoreCase(op) || after.isEmpty || before.isEmpty) {
      return null
    }
    // 过滤数据
    val before_jgsm = before.get("JGSM").getOrElse("").trim
    val after_jgsm = after.get("JGSM").getOrElse("").trim

    val before_jgsm_list = List("待申报","申报中")
    if (after_jgsm.equalsIgnoreCase("已申报") && before_jgsm_list.contains(before_jgsm)) {
      val tdrwtRecord = TdrwtRecord(
        after.get("KHH").getOrElse(""),
        after.get("WTH").getOrElse("0"),
        after.get("YYB").getOrElse(""),
        after.get("WTFS").getOrElse(""),
        after.get("WTGY").getOrElse(""),
        after.get("BZ").getOrElse(""),
        after.get("CXWTH").getOrElse(""),
        after.get("WTSL").getOrElse("0").toInt,
        BigDecimal(after.get("WTJG").getOrElse("0"))
      )
      val channel = tdrwtRecord.matchClassify(
        RuleVaildUtil.getClassifyListByRuleName(Constants.RULE_TX_CHANNEL_CLASSIFY),
        typeOf[TdrwtRecord])
      tdrwtRecord.channel = channel
      tdrwtRecord
    } else {
      null
    }
  }

  /**
    * 转换记录 --> TSSCJ
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTsscjRecord(message: String): TsscjRecord = {
    val (tableName, op, after, owner) = parseAfterJsonStringToMap(message)
    if (!"TSSCJ".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
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
    val (tableName, op, after, owner) = parseAfterJsonStringToMap(message)
    if (!"TKHXX".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty || !"CIF".equalsIgnoreCase(owner)) {
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
    * 转换记录 --> TJGMXLS
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTjgmxlsRecord(message: String): TjgmxlsRecord = {

    val (tableName, op, after, owner) = parseAfterJsonStringToMap(message)
    if (!"TJGMXLS".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
      return null
    }
    // TJGMXLS KHH,LSH,YYB,S1,S11,S12,S13
    TjgmxlsRecord(
      after.get("KHH").getOrElse(""),
      after.get("LSH").getOrElse(""),
      after.get("YYB").getOrElse(""),
      BigDecimal(after.get("S1").getOrElse("0")),
      BigDecimal(after.get("S11").getOrElse("0")),
      BigDecimal(after.get("S12").getOrElse("0")),
      BigDecimal(after.get("S13").getOrElse("0"))
    )
  }

  /**
    * 转换记录 --> TDRZJMX
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTdrzjmxRecord(message: String): TdrzjmxRecord = {
    val (tableName, op, after, owner) = parseAfterJsonStringToMap(message)
    if (!"TDRZJMX".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
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
  def getAfterInfoFromKafkaRecord(message: Map[String, Any]): (String, String, Map[String, String], String) = {

    val after = message.get("after").getOrElse(Map()).asInstanceOf[Map[String, String]]
    var op = message.get("optype").getOrElse("").asInstanceOf[String]
    var tableName = message.get("name").getOrElse("").asInstanceOf[String]
    val owner = message.get("owner").getOrElse("").asInstanceOf[String]
    (tableName, op, after, owner)
  }

  /**
    * 解析KafkaRecord 获取所有信息
    *
    * @param message
    * @return
    */
  def getAllInfoFromKafkaRecord(message: Map[String, Any]): (String, String, String, Map[String, String], Map[String, String]) = {

    val after = message.get("after").getOrElse(Map()).asInstanceOf[Map[String, String]]
    val before = message.get("before").getOrElse(Map()).asInstanceOf[Map[String, String]]
    var op = message.get("optype").getOrElse("").asInstanceOf[String]
    var tableName = message.get("name").getOrElse("").asInstanceOf[String]
    val owner = message.get("owner").getOrElse("").asInstanceOf[String]
    (tableName, op, owner, after, before)
  }

  /**
    *
    * 将 json字符串转Map对象
    *
    * @param message
    */
  def parseAfterJsonStringToMap(message: String): (String, String, Map[String, String], String) = {
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
        ("", "", Map(), "")
      }
    }
  }

  /**
    *
    * 将 json字符串转Map对象
    *
    * @param message
    */
  def parseAllJsonStringToMap(message: String): (String, String, String, Map[String, String], Map[String, String]) = {
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
        ("", "", "", Map(), Map())
      }
    }
  }
}
