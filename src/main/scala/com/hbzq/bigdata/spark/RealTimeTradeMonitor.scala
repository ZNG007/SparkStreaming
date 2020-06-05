package com.hbzq.bigdata.spark

import java.time.LocalDateTime
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.operator.rdd._
import com.hbzq.bigdata.spark.operator.runnable.FlushRedisToMysqlTask.channels
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
    var ssc: StreamingContext = startApp(topics, kafkaParams)
    // 启动调度任务
    val scheduler = startScheduleTask()
    if (!ssc.awaitTerminationOrTimeout(DateUtil.getDurationTime())) {
      logger.warn(
        s"""
           |====================
           |Begin to stop APP
           |${LocalDateTime.now()}
           |====================
        """.stripMargin)
      scheduler.shutdown()
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
    // 获取spark运行时环境
    var (sparkContext, spark, ssc) = SparkUtil.getSparkStreamingRunTime(ConfigurationManager.getInt(Constants.SPARK_BATCH_DURATION))
    // 获取汇率表
    //    val exchangeMap = SparkUtil.getExchangeRateFromHive(spark)
    val exchangeMap: Map[String, BigDecimal] = Map()
    // 广播变量
    val exchangeMapBC = sparkContext.broadcast(exchangeMap)


    val inputStream = SparkUtil.getInputStreamFromKafkaByMysqlOffset(ssc, topics, kafkaParams)

    inputStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.repartition(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM))
      // 提前过滤  只关心INSERT操作
      val inputRdd = rdd.filter(message => {
        val value = message.value()
        StringUtils.isNotEmpty(value) && (value.contains("INSERT"))
      })
        .map(_.value())
      // 持久化
      inputRdd.persist(StorageLevel.MEMORY_AND_DISK)
      // 计算委托相关的业务 委托笔数  委托金额  委托客户数
      val tdrwt: Map[String, (Int, BigDecimal)] = TdrwtOperator(inputRdd, exchangeMapBC).compute().toMap
      // 计算成交相关的业务 成交笔数  成交金额  成交客户数
      val tsscj: Map[String, (Int, BigDecimal)] = TsscjOperator(inputRdd, exchangeMapBC).compute().toMap
      // 新增客户数
      val tkhxx: Map[String, Int] = TkhxxOperator(inputRdd).compute().toMap
      // 计算净佣金
      val tjgmxls: Map[String, BigDecimal] = TjgmxlsOperator(inputRdd).compute().toMap
      // 计算客户转入转出
      val tdrzjmx: Map[String, BigDecimal] = TdrzjmxOperator(inputRdd, exchangeMapBC).compute().toMap
      // 事物更新offset 及结果到Mysql
      DB.localTx(implicit session => {
        val timestamp = DateUtil.getNowTimestamp()
        // tdrwt + tsscj  trade_state
        var tradeStates: List[List[Any]] = List()
        for (channel <- channels.keySet) {
          var (wt_tx_count, wt_tx_sum) = tdrwt.getOrElse(channel, (0, BigDecimal(0)))
          var (cj_tx_count, cj_tx_sum) = tsscj.getOrElse(channel, (0, BigDecimal(0)))
          val _channel = channels.get(channel).get
          tradeStates ::= (_channel :: wt_tx_count :: wt_tx_sum :: cj_tx_count :: cj_tx_sum :: timestamp :: _channel :: wt_tx_count :: wt_tx_sum :: cj_tx_count :: cj_tx_sum :: timestamp :: Nil)
        }
        tradeStates.foreach(tradeState => {
          SQL(ConfigurationManager.getProperty(Constants.FLUSH_REDIS_TO_MYSQL_TRADE_STATE_SQL))
            .bind(tradeState: _*).update().apply()
        })
        // jyj_state
        var jyjStates: List[List[Any]] = List()
        for (yyb: String <- tjgmxls.keySet) {
          val jyj = tjgmxls.getOrElse(yyb, BigDecimal(0))
          jyjStates ::= (yyb :: jyj :: timestamp :: yyb :: jyj :: timestamp :: Nil)
        }
        jyjStates.foreach(jyjState => {
          SQL(ConfigurationManager.getProperty(Constants.FLUSH_REDIS_TO_MYSQL_JYJ_STATE_SQL))
            .bind(jyjState: _*).update().apply()
        })
        // other_state
        val new_jg_count = tkhxx.getOrElse(TkhxxOperator.GR, 0)
        val new_gr_count = tkhxx.getOrElse(TkhxxOperator.JG, 0)
        val zr_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZR, BigDecimal(0))
        val zc_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZC, BigDecimal(0))
        var otherStates: List[List[Any]] = List()
        otherStates ::= (1:: new_jg_count :: new_gr_count :: zr_sum :: zc_sum :: timestamp :: 1:: new_jg_count :: new_gr_count :: zr_sum :: zc_sum :: timestamp :: Nil)
        otherStates.foreach(otherState => {
          SQL(ConfigurationManager.getProperty(Constants.FLUSH_REDIS_TO_MYSQL_OTHER_STATE_SQL))
            .bind(otherState: _*).update().apply()
        })
        // Kafka Offset
        var kafkaOffsets: List[List[Any]] = List()
        offsetRanges.foreach(offsetRange => {
          SQL(ConfigurationManager.getProperty(Constants.KAFKA_MYSQL_INSERT_OFFSET_SQL))
            .bind(offsetRange.topic, ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID)
              , offsetRange.partition, offsetRange.untilOffset).update().apply()
        })
      })

    })
    ssc.start()
    ssc
  }

  /**
    * 调度任务
    *
    * @return
    */
  private def startScheduleTask():ScheduledExecutorService = {
    // 初始化Redis 删除Key调度线程池
    val scheduler = ThreadUtil.getSingleScheduleThreadPool(1)

    // Mysql数据同步调度线程池

    scheduler.scheduleWithFixedDelay(new FlushRedisToMysqlTask(),
      60,
      ConfigurationManager.getInt(Constants.FLUSH_REDIS_TO_MYSQL_SCHEDULE_INTERVAL),
      TimeUnit.SECONDS
    )
    scheduler
  }

  private var channels: Map[String, String] = FlushRedisToMysqlTask.channels
}
