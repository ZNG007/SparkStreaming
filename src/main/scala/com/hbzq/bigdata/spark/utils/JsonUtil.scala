package com.hbzq.bigdata.spark.utils

import java.io.File

import com.hbzq.bigdata.spark.domain.TdrwtRecord
import com.owlike.genson.defaultGenson._
import com.owlike.genson.Genson
import org.apache.commons.io.FileUtils

/**
  * describe:
  * create on 2020/05/27
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */


object JsonUtil {

  /**
    * 读取Json文件获取
    *
    * @param fileName 文件路径
    * @return
    */
  def parseRuleFile(fileName: String): Map[String, List[Map[String, List[String]]]] = {
    fromJson[Map[String, List[Map[String, List[String]]]]](FileUtils.readFileToString(new File(fileName), "UTF-8"))
  }


  /**
    * 获取 （表名，记录操作类型）
    *
    * @param message
    * @return
    */
  def getTableNameAndOperatorFromKakfaRecord(message: String): (String, String) = {
    val record = fromJson[Map[String, Any]](message)
    val optype = record.get("optype").asInstanceOf[String]
    val name = record.get("name").asInstanceOf[String]
    (name, optype)
  }

  /**
    * 转换记录
    *
    * @param message
    * @return
    */
  def parseKakfaRecordToTdrwtRecord[T](message: String): TdrwtRecord = {
    val record = fromJson[Map[String, Any]](message.toUpperCase)
    val after = record.get("AFTER").asInstanceOf[Map[String, String]]
    // "KHH","WTH","YYB","WTFS", "WTGY", "CJJE","BZ","CXWTH"

    val tdrwtRecord = TdrwtRecord(
      after.get("KHH").getOrElse(""),
      after.get("WTH").getOrElse(""),
      after.get("YYB").getOrElse(""),
      after.get("WTFS").getOrElse(""),
      after.get("WTGY").getOrElse(""),
      after.get("CJJE").getOrElse(""),
      after.get("BZ").getOrElse(""),
      after.get("CXWTH").getOrElse("")
    )
    val channel = tdrwtRecord.matchClassify(RuleVaildUtil.getClassifyListByRuleName("tx_channel"))
    tdrwtRecord.channel = channel
    tdrwtRecord
  }

}
