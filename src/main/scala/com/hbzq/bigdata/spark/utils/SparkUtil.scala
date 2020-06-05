package com.hbzq.bigdata.spark.utils

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConverters._



/**
  * describe:
  * create on 2020/05/27
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object SparkUtil {
  private[this] val logger = Logger.getLogger(SparkUtil.getClass)

  /**
    * 获取运行时环境
    *
    * @param duration
    * @return
    */
  def getSparkStreamingRunTime(duration: Int): (SparkContext, SparkSession, StreamingContext) = {
    val spark = SparkSession
      .builder
      .enableHiveSupport()
      .getOrCreate()
    val conf = spark.sparkContext.getConf
    val sparkContext = spark.sparkContext
    val ssc = new StreamingContext(sparkContext, Seconds(duration))
    sparkContext.setLogLevel("WARN")
    (sparkContext, spark, ssc)
  }

  /**
    * 获取kafka 数据流
    *
    * @param ssc
    * @param topics
    * @param kafkaParams
    * @return
    */
  def getInputStreamFromKafka(ssc: StreamingContext,
                              topics: Array[String],
                              kafkaParams: Map[String, Object]): InputDStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
  }

  /**
    * 获取汇率信息
    *
    * @param spark
    * @return
    */
  def getExchangeRateFromHive(spark: SparkSession): Map[String, BigDecimal] = {
    var res: Map[String, BigDecimal] = Map()
    val exchange = spark.sql(ConfigurationManager.getProperty(Constants.DIM_EXCHANGE_RATE_SQL))
      .collectAsList().asScala
      .foreach(row => {
        res += (row.getString(0).trim.toUpperCase -> BigDecimal(row.getDouble(1)))
      })
    logger.warn(
      s"""
        |========
        |ExchangeRateFromHive
        |$res
        |========
      """.stripMargin)
    res
  }

}
