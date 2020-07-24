package com.hbzq.bigdata.spark.config

import java.io.{FileInputStream, IOException, InputStream}
import java.util.Properties

import com.hbzq.bigdata.spark.utils.JsonUtil
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


/**
  * describe:
  * create on 2020/05/27
  *
  * 配置读取类
  *
  * @author hqbhoho
  * @version [v1.0]
  *
  */
object ConfigurationManager {
  private val prop = new Properties()

  /**
    * 加载额外的配置文件
    * 通过--file参数指定文件
    *
    * @return
    */
  def initConfig(fileName: String): Properties = {
    var in: InputStream = null
    try {
      in = ConfigurationManager.getClass.getClassLoader.getResourceAsStream(fileName)
      prop.load(in)
    } catch {
      case ex: IOException =>
    } finally {
      if (in != null) {
        in.close()
      }
    }
    prop
  }

  initConfig("config.properties")

  /**
    * 获取字符串类型配置参数
    *
    * @param key 参数属性
    * @return 字符串类型配置参数值
    */
  def getProperty(key: String): String = {
    prop.getProperty(key)
  }

  /**
    * 获取整型配置参数
    *
    * @param key 参数属性
    * @return 整型配置参数值
    */
  def getInt(key: String): Int = {
    val value = prop.getProperty(key)
    try {
      value.toInt
    } catch {
      case ex: Exception => {
        throw new RuntimeException(s"invalid string:$value cast to int")
      }
    }
  }

  /**
    * 获取布尔型配置参数
    *
    * @param key 参数属性
    * @return 布尔型配置参数值
    */
  def getBoolean(key: String): Boolean = {
    val value: String = prop.getProperty(key)
    try {
      value.toBoolean
    } catch {
      case ex: Exception => {
        throw new RuntimeException(s"invalid string:$value  cast to boolean")
      }
    }
  }

  /**
    * 获取Long类型配置参数
    *
    * @param key 参数属性
    * @return Long类型配置参数值
    */
  def getLong(key: String): Long = {
    val value = prop.getProperty(key)
    try {
      value.toLong
    } catch {
      case ex: Exception => {
        throw new RuntimeException(s"invalid string: $value cast to long")
      }
    }
  }

  /**
    * 获取配置项  并封装成Map对象
    *
    */
  def getRedisConfig(): Map[String, Any] = {
    val redisConf = Map(
      Constants.REDIS_HOSTS -> ConfigurationManager.getProperty(Constants.REDIS_HOSTS).split(Constants.DELIMITER).toSet,
      Constants.REDIS_MASTER -> ConfigurationManager.getProperty(Constants.REDIS_MASTER),
      Constants.REDIS_TIMEOUT -> ConfigurationManager.getInt(Constants.REDIS_TIMEOUT),
      Constants.REDIS_MAX_TOTAL -> ConfigurationManager.getInt(Constants.REDIS_MAX_TOTAL),
      Constants.REDIS_MAX_IDLE -> ConfigurationManager.getInt(Constants.REDIS_MAX_IDLE),
      Constants.REDIS_MIN_IDLE -> ConfigurationManager.getInt(Constants.REDIS_MIN_IDLE),
      Constants.REDIS_TEST_ON_BORROW -> ConfigurationManager.getBoolean(Constants.REDIS_TEST_ON_BORROW),
      Constants.REDIS_TEST_ON_RETURN -> ConfigurationManager.getBoolean(Constants.REDIS_TEST_ON_RETURN),
      Constants.REDIS_MAX_WAIT_MILLIS -> ConfigurationManager.getInt(Constants.REDIS_MAX_WAIT_MILLIS)
    )
    redisConf
  }

  /**
    * 获取Kafka参数
    *
    * @return
    */
  def getKafkaConfig(): (Array[String], Map[String, Object]) = {
    val topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS).split(Constants.DELIMITER)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS),
      "key.deserializer" -> classOf[StringDeserializer],
      "key.serializer" -> classOf[StringSerializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "value.serializer" -> classOf[StringSerializer],
      "group.id" -> ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID),
      "auto.offset.reset" -> ConfigurationManager.getProperty(Constants.KAFKA_AUTO_OFFSET_RESET),
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "acks" -> "-1"
    )
    (topics, kafkaParams)
  }

  /**
    * 获取分类规则
    *
    * @return
    */
  def getRecordRules(): Map[String, Map[String, List[Map[String, List[String]]]]] = {
    var rules: Map[String, Map[String, List[Map[String, List[String]]]]] = Map()
    ConfigurationManager.getProperty(Constants.RULES_LIST).split(",").foreach(
      ruleName => {
//        val rule = JsonUtil.parseRuleFile(s"""E:\\scalaProjects\\realtime-trade-monitor\\src\\main\\resources\\$ruleName.json""")
        val rule = JsonUtil.parseRuleFile(s"""$ruleName.json""")
        rules += (ruleName -> rule)
      }
    )
    rules
  }
}
