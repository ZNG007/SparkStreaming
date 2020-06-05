package com.hbzq.bigdata.spark.utils

import java.io.File

import com.hbzq.bigdata.spark.config.Constants
import com.hbzq.bigdata.spark.domain._
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
  def parseKakfaRecordToTdrwtRecord(message:String): TdrwtRecord = {
    val (tableName, op, after) = parseJsonStringToMap(message)
    if(!"TDRWT".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty){
      return null
    }
    val tdrwtRecord = TdrwtRecord(
      after.get("KHH").getOrElse(""),
      after.get("WTH").getOrElse(""),
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
  }

  /**
    * 转换记录 --> TSSCJ
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTsscjRecord(message:  String): TsscjRecord = {
    val (tableName, op, after) = parseJsonStringToMap(message)
    if(!"TSSCJ".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty){
      return null
    }
    // TSSCJ "KHH","WTH","YYB","WTFS","WTGY","BZ","CJJE"
    val tsscjRecord = TsscjRecord(
      after.get("KHH").getOrElse(""),
      after.get("CJBH").getOrElse(""),
      after.get("YYB").getOrElse(""),
      after.get("WTFS").getOrElse(""),
      after.get("WTGY").getOrElse(""),
      after.get("BZ").getOrElse(""),
      BigDecimal(after.get("CJJE").getOrElse("0"))
    )
    val channel = tsscjRecord.matchClassify(
      RuleVaildUtil.getClassifyListByRuleName(Constants.RULE_TX_CHANNEL_CLASSIFY),
      typeOf[TsscjRecord])
    tsscjRecord.channel = channel
    tsscjRecord
  }

  /**
    * 转换记录 --> TKHXX
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTkhxxRecord(message:String): TkhxxRecord = {
    val (tableName, op, after) = parseJsonStringToMap(message)
    if(!"TKHXX".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty){
      return null
    }
    // CUSTOMER.TKHXX  KHH, KHRQ, JGBZ
   TkhxxRecord(
     after.get("KHH").getOrElse(""),
     after.get("KHRQ").getOrElse("0").toInt,
     after.get("JGBZ").getOrElse("-1").toInt
    )
  }

  /**
    * 转换记录 --> TJGMXLS
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTjgmxlsRecord(message: String): TjgmxlsRecord = {

    val (tableName, op, after) = parseJsonStringToMap(message)
    if(!"TJGMXLS".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty){
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
    val (tableName, op, after) = parseJsonStringToMap(message)
    if(!"TDRZJMX".equalsIgnoreCase(tableName) || !"INSERT".equalsIgnoreCase(op) || after.isEmpty){
      return null
    }

    // TDRZJMX KHH,LSH,YWKM,BZ,SRJE,FCJE
    TdrzjmxRecord(
      after.get("KHH").getOrElse(""),
      after.get("LSH").getOrElse(""),
      after.get("YWKM").getOrElse(""),
      after.get("BZ").getOrElse(""),
      BigDecimal(after.get("SRJE").getOrElse("0")),
      BigDecimal(after.get("FCJE").getOrElse("0"))
    )
  }

  /**
    * 解析KafkaRecord 获取有用信息
    *
    * @param message
    * @return
    */
  def getInfoFromKafkaRecord(message: Map[String,Any]): (String, String, Map[String, String]) = {

    val after = message.get("after").getOrElse(Map()).asInstanceOf[Map[String, String]]
    var op = message.get("optype").getOrElse("").asInstanceOf[String]
    var tableName = message.get("name").getOrElse("").asInstanceOf[String]
    (tableName, op, after)
  }

  /**
    *
    * 将 json字符串转Map对象
    *
    * @param message
    */
  def parseJsonStringToMap(message: String): (String, String, Map[String, String]) = {
    try {
      val parseMap = fromJson[Map[String, Any]](message)
      getInfoFromKafkaRecord(parseMap)
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
        ("","",Map())
      }
    }
  }
}
