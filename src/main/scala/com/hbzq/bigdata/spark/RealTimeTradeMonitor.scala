package com.hbzq.bigdata.spark

import java.time.LocalDateTime
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.operator.rdd._
import com.hbzq.bigdata.spark.operator.runnable.{FlushRedisToMysqlTask, RedisDelKeyTask}
import com.hbzq.bigdata.spark.utils._
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import scalikejdbc.{DB, SQL}


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
  private[this] val logger = Logger.getLogger(RealTimeTradeMonitor.getClass)

  def main(args: Array[String]): Unit = {
    // 获取Kafka配置
    var (topics, kafkaParams) = ConfigurationManager.getKafkaConfig()
    // 启动  先删除Redis  key
    RedisDelKeyTask().run()
    var ssc: StreamingContext = startApp(topics, kafkaParams)
    if (!ssc.awaitTerminationOrTimeout(DateUtil.getDurationTime())) {
      logger.warn(
        s"""
           |====================
           |Begin to stop APP
           |${LocalDateTime.now()}
           |====================
        """.stripMargin)
      ssc.stop(true, true)
    }
  }

  /**
    * 启动Sparkr任务
    *
    * @param topics
    * @param kafkaParams
    * @return
    */
  private def startApp(topics: Array[String], kafkaParams: Map[String, Object]): StreamingContext = {
    val now = DateUtil.getFormatNowDate()
    // 获取spark运行时环境
    var (sparkContext, spark, ssc) = SparkUtil.getSparkStreamingRunTime(ConfigurationManager.getInt(Constants.SPARK_BATCH_DURATION))
    // 获取汇率表
    val exchangeMap = SparkUtil.getExchangeRateFromHive(spark)
    //    val exchangeMap:Map[String, BigDecimal] =Map()
    // 广播变量
    val exchangeMapBC = sparkContext.broadcast(exchangeMap)
    // 获取输入流
    val inputStream = SparkUtil.getInputStreamFromKafkaByMysqlOffset(ssc, topics, kafkaParams)
    inputStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 提前过滤
      val inputRdd = rdd
        .filter(message => StringUtils.isNotEmpty(message.value()))
        .repartition(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM))
        .persist(StorageLevel.MEMORY_ONLY_SER)
      // 计算委托相关的业务  委托笔数  委托金额  (yyb , channel)
      val tdrwt: Map[(String, String), (Int, BigDecimal)] = TdrwtOperator(inputRdd, exchangeMapBC).compute().toMap
      // 新增客户数
      val tkhxx: Map[String, Int] = TkhxxOperator(inputRdd).compute().toMap
      // 计算客户转入转出
      val tdrzjmx: Map[String, BigDecimal] = TdrzjmxOperator(inputRdd, exchangeMapBC).compute().toMap
      // 计算成交相关的业务 成交笔数  成交金额  佣金   (yyb , channel)
      val tsscj: Map[(String, String), (Int, BigDecimal, BigDecimal)] = TsscjOperator(inputRdd, exchangeMapBC).compute().toMap

      // 事物更新offset 及结果到Mysql
      DB.localTx(implicit session => {
        val timestamp = DateUtil.getNowTimestamp()
        var nowTradeStates: List[List[Any]] = List()
        if (!tdrwt.isEmpty || !tsscj.isEmpty) {
          val keys = tdrwt.keySet ++ tsscj.keySet;
          for ((yyb, channel) <- keys) {
            var (now_wt_tx_count, now_wt_tx_sum) = tdrwt.getOrElse((yyb, channel), (0, BigDecimal(0)))
            var (cj_tx_count, cj_tx_sum, jyj) = tsscj.getOrElse((yyb, channel), (0, BigDecimal(0), BigDecimal(0)))
            //            val _channel = channels.get(channel).get
            if (now_wt_tx_count != 0 || cj_tx_count != 0) {
              nowTradeStates ::= (channel :: yyb :: now_wt_tx_count :: now_wt_tx_sum :: cj_tx_count :: cj_tx_sum :: jyj :: timestamp :: now :: channel :: yyb :: now_wt_tx_count :: now_wt_tx_sum :: cj_tx_count :: cj_tx_sum :: jyj :: timestamp :: now :: Nil)
            }
          }
          if (!nowTradeStates.isEmpty) {
            nowTradeStates.foreach(tradeState => {
              SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_NOW_TRADE_STATE_SQL))
                .bind(tradeState: _*).update().apply()
            })
          }
        }
        // other_state
        val new_jg_count = tkhxx.getOrElse(TkhxxOperator.GR, 0)
        val new_gr_count = tkhxx.getOrElse(TkhxxOperator.JG, 0)
        val zr_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZR, BigDecimal(0))
        val zc_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZC, BigDecimal(0))
        if (new_jg_count != 0 || new_gr_count != 0 || zr_sum != 0 || zc_sum != 0) {
          var otherStates: List[List[Any]] = List()
          otherStates ::= (new_jg_count :: new_gr_count :: zr_sum :: zc_sum :: timestamp :: now :: new_jg_count :: new_gr_count :: zr_sum :: zc_sum :: timestamp :: now :: Nil)
          otherStates.foreach(otherState => {
            SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_OTHER_STATE_SQL))
              .bind(otherState: _*).update().apply()
          })
        }
        // Kafka Offset
        var kafkaOffsets: List[List[Any]] = List()
        offsetRanges.foreach(offsetRange => {
          SQL(ConfigurationManager.getProperty(Constants.KAFKA_MYSQL_INSERT_OFFSET_SQL))
            .bind(offsetRange.topic, ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID)
              , offsetRange.partition, offsetRange.untilOffset).update().apply()
        })
      })
      // 刷新 Redis 统计数据到Msyql
      FlushRedisToMysqlTask().run()
    })
    ssc.start()
    ssc
  }
}
