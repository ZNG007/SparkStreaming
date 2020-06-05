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
    // 获取spark运行时环境
    var (sparkContext, spark, ssc) = SparkUtil.getSparkStreamingRunTime(ConfigurationManager.getInt(Constants.SPARK_BATCH_DURATION))
    // 获取汇率表
    val exchangeMap = SparkUtil.getExchangeRateFromHive(spark)
    //    val exchangeMap: Map[String, BigDecimal] = Map()
    // 广播变量
    val exchangeMapBC = sparkContext.broadcast(exchangeMap)
    // 开启调度任务
    startScheduleTask()
    // begin from the the offsets committed to the database
    /*val offsets = MysqlJdbcUtil.executeQuery(s"""select topic,partition,offset from test.offset where group_id=${kafkaParams.get("group.id").get}""")
    val fromOffsets = offsets.map(rs =>
      new TopicPartition(rs.getString(1), rs.getInt(2)) -> rs.getLong(3)).toMap
    val input = KafkaUtils.createDirectStream[String, String](
       ssc,
       PreferConsistent,
       ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
     )*/

    val inputStream = SparkUtil.getInputStreamFromKafka(ssc, topics, kafkaParams)

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
      TdrwtOperator(inputRdd, exchangeMapBC).compute()
      // 计算成交相关的业务 成交笔数  成交金额  成交客户数
      TsscjOperator(inputRdd, exchangeMapBC).compute()
      // 新增客户数
      TkhxxOperator(inputRdd).compute()
      // 计算净佣金
      TjgmxlsOperator(inputRdd).compute()
      // 计算客户转入转出
      TdrzjmxOperator(inputRdd, exchangeMapBC).compute()
      // 先测试提交给kafka
      inputStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    ssc.start()
    ssc
  }

  /**
    * 调度任务
    *
    * @return
    */
  private def startScheduleTask(): ScheduledExecutorService = {
    // 初始化Redis 删除Key调度线程池
    val scheduler = ThreadUtil.getSingleScheduleThreadPool(2)
    // 定期清除Redis中的key
    scheduler.scheduleWithFixedDelay(new RedisDelKeyTask(),
      0,
      ConfigurationManager.getInt(Constants.REDIS_KEY_DEL_SCHEDULE_INTERVAL),
      TimeUnit.SECONDS
    )
    // Mysql数据同步调度线程池
    scheduler.scheduleWithFixedDelay(new FlushRedisToMysqlTask(),
      60,
      ConfigurationManager.getInt(Constants.FLUSH_REDIS_TO_MYSQL_SCHEDULE_INTERVAL),
      TimeUnit.SECONDS
    )

    scheduler
  }
}
