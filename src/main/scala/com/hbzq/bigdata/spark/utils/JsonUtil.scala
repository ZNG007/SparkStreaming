package com.hbzq.bigdata.spark.utils

import java.io.File

import com.hbzq.bigdata.spark.config.Constants
import com.hbzq.bigdata.spark.domain._
import com.hbzq.bigdata.spark.operator.rdd.{TdrzjmxOperator, TkhxxOperator}
import com.owlike.genson.defaultGenson._
import org.apache.commons.io.FileUtils
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

}
