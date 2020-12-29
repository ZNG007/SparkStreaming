package com.hbzq.bigdata.spark.utils

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain._
import com.hbzq.bigdata.spark.operator.rdd.{TdrzjmxOperator, TkhxxOperator}
import org.apache.log4j.Logger
import org.json4s.jackson.Serialization
import DateUtil.tranTimeToString

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
          data.get("WTH").getOrElse("0"),
          data.get("GDH").getOrElse("")
        )
        tsscjRecord.version = data.get("VERSION").getOrElse("0").toInt
        tsscjRecord
      }
      case "SECURITIES_TYWQQ" =>{
        val data = parseJsonObjectToMap(parseMap.getJSONObject("BASERECORD"))
        var khh=data.get("KHH").getOrElse("0")
        logger.warn("***********ywqqdata *********"+data)
        if(khh==null){
          khh="nvl"
        }
        val tywqqRecord= TywqqRecord(
          "UPDATE",
          "2",
          data.get("ID").getOrElse("nvl"),
          khh,
          data.get("SQRQ").getOrElse("0").toInt,
          data.get("YYB").getOrElse(""),
          data.get("CLZT").getOrElse("0"),
          DateUtil.getFormatNowDate().toString
        )
        tywqqRecord.version = data.get("VERSION").getOrElse("0").toInt
        tywqqRecord
      }
      case "SECURITIES_TYWSQLSKH" =>{
        val data = parseJsonObjectToMap(parseMap.getJSONObject("BASERECORD"))

        var wth=data.getOrElse("WTH","nvl")
        var khh=data.getOrElse("KHH","nvl")
        var cljg=data.getOrElse("CLJG","nvl")
        var cpfl= data.getOrElse("CPFL","nvl")
        var cpid=data.getOrElse("CPID","nvl")
        logger.warn("ywsqlsdata "+data)
        if(wth==null){
          wth="nvl"
        }
        if(khh==null){
          khh="nvl"
        }
        if(cljg==null){
          cljg="nvl"
        }
        if(cpfl==null){
          cpfl="nvl"
        }
        if(cpid==null){
          cpid="nvl"
        }

        val tFPYwsqlsRecord = TFPYwsqlsRecord(
          "UPDATE",
          "1",
          wth,
          khh,
          data.getOrElse("YWDM","nvl"),
          data.getOrElse("CLZT","nvl"),
          cljg,
          cpfl,
          BigDecimal(data.getOrElse("YWJE","0")),
          data.getOrElse("DJRQ","nvl"),
          cpid,
          DateUtil.getFormatNowDate().toString
        )
        tFPYwsqlsRecord.version = data.get("VERSION").getOrElse("0").toInt
        tFPYwsqlsRecord
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
    // Only Care Insert
  //  logger.warn(after+"==============TDRWT 解析========================")
    if(!"INSERT".equalsIgnoreCase(op) || after.isEmpty){
      return null;
    }
    val khh = after.get("KHH").getOrElse("")
    val wth = after.get("WTH").getOrElse("0")
    val yyb = after.get("YYB").getOrElse("")
    val wtfs = after.get("WTFS").getOrElse("")
    val wtgy = after.get("WTGY").getOrElse("")
    val bz = after.get("BZ").getOrElse("")
    val wtsl = after.get("WTSL").getOrElse("0")
    val wtjg = BigDecimal(after.get("WTJG").getOrElse("0"))
    val sbjg = after.get("SBJG").getOrElse("").trim
    val cxbz = after.getOrElse("CXBZ","")
    val jslx = after.getOrElse("JSLX","")
    val zqlb = after.getOrElse("ZQLB","")
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
      sbjg,
      cxbz,
      jslx,
      zqlb
    )
    val channel = tdrwtRecord.matchClassify(
      RuleVaildUtil.getClassifyListByRuleName(Constants.RULE_TX_CHANNEL_CLASSIFY),
      typeOf[TdrwtRecord])
    tdrwtRecord.channel = channel
    tdrwtRecord
    // Insert and Update
    /*if (after.isEmpty) {
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
    }*/
  }

  /**
    * 转换记录 --> TSSCJ
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTsscjRecord(message: String): TsscjRecord = {

    val (op, after,rowid,jstime) = parseAfterJsonStringToMap(message)
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
      after.get("WTH").getOrElse("0"),
      after.get("GDH").getOrElse("")
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

    val (op, after,rowid,jstime) = parseAfterJsonStringToMap(message)
    var xhrq=after.get("XHRQ").getOrElse("0")
    val unixtime =jstime.substring(0,13)
    val time=tranTimeToString(unixtime)
    if (after.isEmpty) {
      return null
    }
    if(xhrq==null){
      xhrq="nvl"
    }
    val jgbz = after.get("KHLB").getOrElse("-1").toInt
    jgbz match {
      case 0 => TkhxxRecord(
        after.get("KHH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        TkhxxOperator.GR,
        after.get("KHZT").getOrElse("0"),
        xhrq,
        time,
        op
      )
      case 1 => TkhxxRecord(
        after.get("KHH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        TkhxxOperator.JG,
        after.get("KHZT").getOrElse("0"),
        xhrq,
        time,
        op
      )
      case _ => TkhxxRecord(
        after.get("KHH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        TkhxxOperator.QT,
        after.get("KHZT").getOrElse("0"),
        xhrq,
        time,
        op
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

    val (op, after,rowid,jstime) = parseAfterJsonStringToMap(message)
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
    * 转换记录-->TFP_YWSQLS 2020.10.20 wf
    *
    * @param message
    * @return
    * */
  def parseKafkaRecordToTFPYwsqlsRecord(message:String): TFPYwsqlsRecord ={
//    logger.warn("***********otc*********"+message)
    val(op, after,rowid,jstime) = parseAfterJsonStringToMap(message)

    var wth=after.getOrElse("WTH","nvl")
    var khh=after.getOrElse("KHH","nvl")
    var cljg=after.getOrElse("CLJG","nvl")
    var cpfl= after.getOrElse("CPFL","nvl")
    var cpid=after.getOrElse("CPID","nvl")
    val unixtime =jstime.substring(0,13)
    val time=tranTimeToString(unixtime)


    logger.warn("***********otc1*********"+op+"rowid  "+rowid+" time "+time+"====="+after)
    if (after.isEmpty){
      return null
    }
    if(khh==null){
      khh="nvl"
    }
    if(cljg==null){
      cljg="nvl"
    }
    if(cpfl==null){
      cpfl="nvl"
    }
    if(cpid==null){
      cpid="nvl"
    }
    //可以试着重写getorelse

    val tFPYwsqlsRecord = TFPYwsqlsRecord(
      op,
      rowid,
      wth,
      khh,
      after.getOrElse("YWDM","nvl"),
      after.getOrElse("CLZT","nvl"),
      cljg,
      cpfl,
      BigDecimal(after.getOrElse("YWJE","0")),
      after.getOrElse("DJRQ","nvl"),
      cpid,
      time
    )
    tFPYwsqlsRecord
  }

  def parseKafkaRecordToTgdzhRecord(message: String) : TgdzhRecord={
    val (op, after,rowid,jstime) = parseAfterJsonStringToMap(message)
    if (!"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
      return null
    }
    val gdzt: String = after.get("GDZT").getOrElse("-1")

    gdzt match {
      case "0" => TgdzhRecord(
        after.get("KHH").getOrElse(""),
        after.get("GDH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        "0",
        after.get("ZHLB").getOrElse("")
      )
      case _ => TgdzhRecord(
        after.get("KHH").getOrElse(""),
        after.get("GDH").getOrElse(""),
        after.get("KHRQ").getOrElse("0").toInt,
        "-1",
        after.get("ZHLB").getOrElse("")
      )
    }
  }


  def parseKafkaRecordToTzjzhRecord(message: String) : TzjzhRecord={
    val (op, after,rowid,jstime) = parseAfterJsonStringToMap(message)
    if (!"INSERT".equalsIgnoreCase(op) || after.isEmpty) {
      return null
    }
    val tzjzhRecord = TzjzhRecord(
      after.get("KHH").getOrElse(""),
      after.get("ZJZH").getOrElse(""),
      after.get("KHRQ").getOrElse("").toInt,
      after.get("ZHZT").getOrElse("")
    )
    tzjzhRecord

  }
  def parseKafkaRecordToTywqqRecord(message: String) : TywqqRecord={
    val (op, after,rowid,jstime) = parseAfterJsonStringToMap(message)
    var khh=after.get("KHH").getOrElse("0")
    val unixtime =jstime.substring(0,13)
    val time=tranTimeToString(unixtime)
    logger.warn("***********ywqq1*********"+"rowid："+rowid+"op："+op+" time "+time+"====="+after)
    if (after.isEmpty) {
      return null
    }
    if(khh==null){
      khh="nvl"
    }
    val tywqqRecord= TywqqRecord(
      op,
      rowid,
      after.get("ID").getOrElse("nvl"),
      khh,
      after.get("SQRQ").getOrElse("0").toInt,
      after.get("YYB").getOrElse(""),
      after.get("CLZT").getOrElse("0"),
      time
    )
    tywqqRecord
  }

  def parseKafkaRecordToTjjzhRecord(message: String) : TjjzhRecord={
    val (op, after,rowid,jstime) = parseAfterJsonStringToMap(message)
    var khh=after.get("KHH").getOrElse("0")
    val unixtime =jstime.substring(0,13)
    val time=tranTimeToString(unixtime)
    logger.warn("***********jjzh1*********"+"rowid："+rowid+"op："+op+" time "+time+"====="+after)
    if (after.isEmpty) {
      return null
    }
    if(khh==null){
      khh="nvl"
    }
    val tjjzhRecord= TjjzhRecord(
      op,
      khh,
      after.get("JJZH").getOrElse("nvl"),
      after.get("KHRQ").getOrElse("0").toInt,
      after.get("ZHZT").getOrElse("0")
    )
    tjjzhRecord
  }

  /**
    * 解析KafkaRecord 获取after信息
    *
    * @param message
    * @return
    */
  def getAfterInfoFromKafkaRecord(message: JSONObject): (String, Map[String, String],String,String) = {
    val after = parseJsonObjectToMap(message.getJSONObject("after"))
    val rowid =message.getString("rowid")
    val jstime =message.getString("jstime")
    var op = message.getString("optype")
    (op, after, rowid,jstime)
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
  def parseAfterJsonStringToMap(message: String): (String, Map[String, String],String,String) = {
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
        ("", Map(),"","")
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
