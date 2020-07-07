package com.hbzq.bigdata.spark.operator.runnable

import java.time.LocalDateTime

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.utils.RedisUtil
import org.apache.log4j.Logger

import scala.collection.JavaConverters._

/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
class RedisDelKeyTask extends Runnable {

  override def run(): Unit = {
    val pattern = ConfigurationManager.getProperty(Constants.REDIS_KEY_DEL_PATTERN)

    if (LocalDateTime.now().getHour == ConfigurationManager.getInt(Constants.REDIS_KEY_DEL_MACTH_HOUR)) {
      val jedis = RedisUtil.getConn
      val keys = jedis.keys(pattern)
      keys.asScala.foreach(jedis.del(_))
      RedisUtil.closeConn(jedis)
    }

  }
}

object RedisDelKeyTask {
  def apply(): RedisDelKeyTask = new RedisDelKeyTask()
}
