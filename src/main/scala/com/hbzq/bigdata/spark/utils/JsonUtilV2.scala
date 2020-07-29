package com.hbzq.bigdata.spark.utils

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain._
import com.hbzq.bigdata.spark.operator.rdd.{TdrzjmxOperator, TkhxxOperator}
import org.apache.log4j.Logger
import org.json4s.jackson.Serialization

import scala.collection.mutable.Map
import scala.reflect.runtime.universe._

/**
  * describe:
  * create on 2020/07/22
  *
  * @author hqbhoho
  * @version [v1.0]
  *
  */
object JsonUtilV2 {
  private[this] val logger = Logger.getLogger(JsonUtilV2.getClass)

  /**
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToSlowMessageRecord(message: String): SlowMessageRecord = {
    val parseMap: fastjson.JSONObject = JSON.parseObject(message.toUpperCase)
    val originTopic = parseMap.getString("ORIGINTOPIC")
    val baseRecord = originTopic match {
      case "SECURITIES_TSSCJ" =>{
        val data = parseJsonObjectToMap(parseMap.getJSONObject("BASERECORD"))
        val tsscjRecord = TsscjRecord(
          data.get("KHH").getOrElse(""),
          data.get("CJBH").getOrElse(""),
          data.get("YYB").getOrElse(""),
          data.get("BZ").getOrElse(""),
          BigDecimal(data.get("CJJE").getOrElse("0")),
          BigDecimal(data.get("S1").getOrElse("0")),
          data.get("CXBZ").getOrElse(""),
          data.get("WTH").getOrElse("0")
        )
        tsscjRecord.version = data.get("VERSION").getOrElse("0").toInt
        tsscjRecord
      }
      case _ => NullRecord()
    }
    SlowMessageRecord(originTopic,baseRecord)
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
        val sbjg = after.get("SBJG").getOrElse("").trim
        val tdrwtRecord = TdrwtRecord(
          op,
          khh,
          wth,
          yyb,
          wtfs,
          wtgy,
          bz,
          wtsl.toInt,
          wtjg,
          sbjg
        )
        val channel = tdrwtRecord.matchClassify(
          RuleVaildUtil.getClassifyListByRuleName(Constants.RULE_TX_CHANNEL_CLASSIFY),
          typeOf[TdrwtRecord])
        tdrwtRecord.channel = channel
        tdrwtRecord
      }
      // UPDATE消息
      case "UPDATE" => {
        val after_sbjg = after.get("SBJG").getOrElse("").trim
        val before_sbjg = before.get("SBJG").getOrElse("").trim
        val wth = before.get("WTH").getOrElse("0")
        if ((!"2".equalsIgnoreCase(before_sbjg)) && "2".equalsIgnoreCase(after_sbjg)) {
          TdrwtRecord(
            op,
            "",
            wth,
            "",
            "",
            "",
            "",
            0,
            BigDecimal(0),
            after_sbjg
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
    val jgbz = after.get("KHLB").getOrElse("-1").toInt
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
  def getAfterInfoFromKafkaRecord(message: JSONObject): (String, Map[String, String]) = {
    val after = parseJsonObjectToMap(message.getJSONObject("after"))
    var op = message.getString("optype")
    (op, after)
  }


  /**
    * 解析KafkaRecord 获取所有信息
    *
    * @param message
    * @return
    */
  def getAllInfoFromKafkaRecord(message: JSONObject): (String, Map[String, String], Map[String, String]) = {

    val after = parseJsonObjectToMap(message.getJSONObject("after"))
    val before = parseJsonObjectToMap(message.getJSONObject("before"))
    var op = message.getString("optype")
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
      val parseMap: fastjson.JSONObject = JSON.parseObject(message)
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
      val parseMap: fastjson.JSONObject = JSON.parseObject(message)
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

  /**
    *
    * @param jsonObj
    * @return
    */
  def parseJsonObjectToMap(jsonObj: JSONObject): Map[String, String] = {
    if (jsonObj == null || jsonObj.size() == 0) return Map()
    val map: Map[String, String] = Map()
    val jsonKey = jsonObj.keySet()
    val iter = jsonKey.iterator()
    while (iter.hasNext) {
      val instance = iter.next()
      val value = jsonObj.getString(instance)
      map.put(instance, value)
    }
    map
  }

  /**
    *
    * @param obj
    * @return
    */
  def parseObjectToJson(obj: BaseRecord): String = {
    implicit val formats = org.json4s.DefaultFormats
    Serialization.write(obj)
  }

}
