package com.hbzq.bigdata.spark

import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.TimeUnit

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain.TdrwtRecord
import java.nio.charset.Charset

import com.google.common.hash.{BloomFilter, Funnels}
import com.hbzq.bigdata.spark.utils.{JsonUtil, RuleVaildUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}


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
        val rule = JsonUtil.parseRuleFile(s"""E:\scalaProjects\realtime-trade-monitor\src\main\resources\$ruleName.json""")
        rules += (ruleName -> rule)
      }
    )

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("RealTimeTradeMonitor")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rulesbroadcast = spark.sparkContext.broadcast(rules)

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.105.1.172:9092")
      .option("subscribe", "orac2kafka2")
      .load()

    df.cache()

    val res = df.filter(row => {
      val value = row.getAs[String]("value")
      val tableNameAndOp = JsonUtil.getTableNameAndOperatorFromKakfaRecord(value)
      tableNameAndOp._1.equalsIgnoreCase("TDRWT") && tableNameAndOp._2.equalsIgnoreCase("UPDATE")
    }).map(row => {
      val value = row.getAs[String]("value")
      RuleVaildUtil.init(rulesbroadcast.value)
      JsonUtil.parseKakfaRecordToTdrwtRecord(value)
    }).filter(record => {
      record.cxwth == "" && record.khh != ""
    }).groupByKey(row => row.channel).mapGroupsWithState[(Long, Long, BigDecimal, LocalDate,BloomFilter[CharSequence]), (String, Long, Long, BigDecimal)] {
      (key:String, iter:Iterator[TdrwtRecord], state:GroupState[(Long, Long, BigDecimal, LocalDate,BloomFilter[CharSequence])]) => {
        val now = LocalDate.now()

        // 清除不是当天的状态
        if (state.exists && now.isEqual(state.get._4)) {
          state.remove()
        }

        if (!state.exists) {
          val b = BloomFilter.create(Funnels.stringFunnel(Charset.forName("utf-8")), 800000, 0.00001)
          state.update((0L, 0L, BigDecimal("0"), LocalDate.now(),b))
        }

        val curState = state.get
        var customNum = state.get._1
        var wtNum = state.get._2
        var wtCount = state.get._3
        val fliter = state.get._5

        while (iter.hasNext) {
          val record = iter.next()
          if(!fliter.mightContain(record.khh)){
            customNum += 1
            fliter.put(record.khh)
          }
          wtNum+= 1
          wtCount += BigDecimal(record.cjje)
        }
        state.update((customNum,wtNum,wtCount,now,fliter))
        (key, customNum, wtNum, wtCount)
      }
    }


    val query = res.writeStream
      .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
