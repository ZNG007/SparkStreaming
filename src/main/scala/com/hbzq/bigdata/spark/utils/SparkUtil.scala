package com.hbzq.bigdata.spark.utils

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val conf = sparkContext.getConf

    // 注册Kyro 序列化类
    conf.registerKryoClasses(
      Array(
        classOf[TdrwtRecord],
        classOf[TsscjRecord],
        classOf[TkhxxRecord],
        classOf[TjgmxlsRecord],
        classOf[TdrzjmxRecord]
      )
    )

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
    * offset 信息保存在Mysql
    *
    * @param ssc
    * @param topics
    * @param kafkaParams
    * @return
    */
  def getInputStreamFromKafkaByMysqlOffset(ssc: StreamingContext,
                                           topics: Array[String],
                                           kafkaParams: Map[String, Object]): InputDStream[ConsumerRecord[String, String]] = {
    // begin from the the offsets committed to the database
    val offsets = MysqlJdbcUtil.executeQuery(ConfigurationManager.getProperty(Constants.KAFKA_MYSQL_QUERY_OFFSET_SQL)
      , List(ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID)))
    val fromOffsets = offsets.map(rs =>
      new TopicPartition(rs.get("topic").get.asInstanceOf[String], rs.get("partition").get.asInstanceOf[Int]) -> rs.get("offset").get.asInstanceOf[Long]).toMap
    if (fromOffsets.isEmpty) {
      getInputStreamFromKafka(ssc, topics, kafkaParams)
    } else {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
      )
    }

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
