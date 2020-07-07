package com.hbzq.bigdata.spark.operator.runnable

import java.sql.Timestamp

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.operator.rdd._
import com.hbzq.bigdata.spark.utils.{DateUtil, MysqlJdbcUtil, RedisUtil}
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._


/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
class FlushRedisToMysqlTask() extends Runnable {

  private[this] val logger = Logger.getLogger(FlushRedisToMysqlTask.getClass)

  override def run(): Unit = {
    val jedis = RedisUtil.getConn()
    val keys = FlushRedisToMysqlTask.keys
    processRealTradeKhhState(jedis, keys)
    RedisUtil.closeConn(jedis)
  }


  /**
    * 刷新有关交易的数据到Mysql
    *
    * @param jedis
    * @param keys
    */
  private def processRealTradeKhhState(jedis: Jedis, keys: Map[String, String]) = {
    // 委托客户数
    var wt_cust_count_map: Map[(Int, String), Int] = getSetSizeFromRedisWithPrefix(FlushRedisToMysqlTask.WT_CUST_COUNT,
      keys, jedis)

    // 成交客户数
    var cj_cust_count_map: Map[(Int, String), Int] = getSetSizeFromRedisWithPrefix(FlushRedisToMysqlTask.CJ_CUST_COUNT,
      keys, jedis)

    var res: List[List[Any]] = List()
    val now = DateUtil.getFormatNowDate()
    for (channel: String <- FlushRedisToMysqlTask.channels.keySet) {
      val _channel = FlushRedisToMysqlTask.channels.get(channel).get
      val now_wt_cust_count = wt_cust_count_map.getOrElse((now, channel), 0)
      val now_cj_cust_count = cj_cust_count_map.getOrElse((now, channel), 0)
      if(now_wt_cust_count != 0 || now_cj_cust_count != 0){
        res ::= (_channel :: now_wt_cust_count :: now_cj_cust_count :: now :: _channel :: now_wt_cust_count :: now_cj_cust_count :: now :: Nil)
      }
    }

    logger.warn(
      s"""
         |=========
         |RealTradeState update to Mysql
         |$res
         |=========
      """.stripMargin)

    MysqlJdbcUtil.executeBatchUpdate(ConfigurationManager.getProperty(Constants.FLUSH_REDIS_TO_MYSQL_TRADE_STATE_KHH_SQL), res)

  }


  /**
    * 根据前缀模式匹配符合条件的Key,并获取值,同时将key中有关Prefix替换成""
    *
    * @param Prefix
    * @param jedis
    * @param keys
    * @return
    */
  def getSetSizeFromRedisWithPrefix(Prefix: String, keys: Map[String, String], jedis: Jedis): Map[(Int, String), Int] = {
    var res: Map[(Int, String), Int] = Map()
    val keyPattrn = keys.get(Prefix).get
    val matchKeys = jedis.keys(s"${keyPattrn}*")
    matchKeys.asScala.foreach(key => {
      val tempValue = jedis.bitcount(key)
      val date = key.replace(keyPattrn, "").split("_")(1).toInt
      val channel = key.replace(keyPattrn, "").split("_")(2)
      val value = res.getOrElse((date, channel), 0) + tempValue
      res += ((date, channel) -> value.toInt)
    })
    res
  }


  /**
    * 根据前缀模式匹配符合条件的Key,并获取值,同时将key中有关Prefix替换成""
    *
    * @param Prefix
    * @param jedis
    * @param keys
    * @return
    */
  def getDataFromRedisWithPrefix(Prefix: String, keys: Map[String, String], jedis: Jedis): Map[String, String] = {
    var res: Map[String, String] = Map()
    val keyPattrn = keys.get(Prefix).get
    val matchKeys = jedis.keys(s"${keyPattrn}*")
    matchKeys.asScala.foreach(key => {
      val value = jedis.get(key)
      res += (key.replace(keyPattrn, "") -> value)
    })
    res
  }

  /**
    *
    * @param name
    * @param keys
    * @param jedis
    * @return
    */
  def getDataFromRedisWithName(name: String, keys: Map[String, String], jedis: Jedis): String = {
    var value = jedis.get(keys.get(name).get)
    if (StringUtils.isEmpty(value)) value = "0"
    value
  }
}

object FlushRedisToMysqlTask {
  private var keys: Map[String, String] = Map()
  var channels: Map[String, String] = Map()
  private val WT_CUST_COUNT: String = "wt_cust_count"
  private val CJ_CUST_COUNT: String = "cj_cust_count"


  private def init(): Unit = {
    keys += (WT_CUST_COUNT -> TdrwtOperator.TDRWT_KHH_PREFIX)
    keys += (CJ_CUST_COUNT -> TsscjOperator.TSSCJ_KHH_PREFIX)
    channels += ("tdx" -> "通达信")
    channels += ("ths" -> "同花顺")
    channels += ("dzh" -> "大智慧")
    channels += ("hbzt" -> "华宝智投")
    channels += ("hbzq" -> "华宝证券")
    channels += ("qt" -> "其他")
    channels += ("undefine" -> "未定义")
  }

  def apply(): FlushRedisToMysqlTask = new FlushRedisToMysqlTask()

  init()
}


