package com.hbzq.bigdata.spark

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain.TsswtRecord
import com.hbzq.bigdata.spark.utils.{JsonUtil, RuleVaildUtil}

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
  def main(args: Array[String]): Unit = {

    ConfigurationManager.initConfig("E:\\scalaProjects\\realtime-trade-monitor\\src\\main\\resources\\config.properties")
    var rules: Map[String, Map[String, List[Map[String, List[String]]]]] = Map()
    ConfigurationManager.getProperty(Constants.RULES_LIST).split(",").foreach(
      ruleName => {
        rules += (ruleName -> JsonUtil.parseRuleFile(s"""E:\\scalaProjects\\realtime-trade-monitor\\src\\main\\resources\\$ruleName.json"""))
      }
    )

    val record = TsswtRecord("90012301","512")
    RuleVaildUtil.init(rules)
    println(RuleVaildUtil.matchClassify(record,"tx_channel"))



  }
}
