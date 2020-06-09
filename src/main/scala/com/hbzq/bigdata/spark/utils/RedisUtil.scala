package com.hbzq.bigdata.spark.utils


import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisSentinelPool}

import scala.collection.JavaConverters._

object RedisUtil {

  private var pool: JedisSentinelPool = null
  private var config: Map[String, Any] = ConfigurationManager.getRedisConfig()

  /**
    * 初始化哨兵机制下的Redis连接池
    *
    * @param hosts
    * @param redisTimeout
    * @param maxTotal
    * @param maxIdle
    * @param minIdle
    * @param testOnBorrow
    * @param testOnReturn
    * @param maxWaitMillis
    */
  private[this] def makePool(hosts: Set[String], master: String, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): JedisSentinelPool = {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)
    poolConfig.setTestOnBorrow(testOnBorrow)
    poolConfig.setTestOnReturn(testOnReturn)
    poolConfig.setMaxWaitMillis(maxWaitMillis)
    new JedisSentinelPool(master, hosts.asJava, poolConfig)
  }


  /**
    * 延迟 获取池子
    *
    * @return
    */
  def getPool(): JedisSentinelPool = {
    if (this.pool == null) {
      RedisUtil.synchronized {
        if (this.pool == null) {
          this.pool = makePool(
            this.config.get(Constants.REDIS_HOSTS).get.asInstanceOf[Set[String]],
            this.config.get(Constants.REDIS_MASTER).getOrElse("").asInstanceOf[String],
            this.config.get(Constants.REDIS_TIMEOUT).getOrElse(10000).asInstanceOf[Int],
            this.config.get(Constants.REDIS_MAX_TOTAL).getOrElse(10).asInstanceOf[Int],
            this.config.get(Constants.REDIS_MAX_IDLE).getOrElse(9).asInstanceOf[Int],
            this.config.get(Constants.REDIS_MIN_IDLE).getOrElse(1).asInstanceOf[Int],
            this.config.get(Constants.REDIS_TEST_ON_BORROW).getOrElse(true).asInstanceOf[Boolean],
            this.config.get(Constants.REDIS_TEST_ON_RETURN).getOrElse(true).asInstanceOf[Boolean],
            this.config.get(Constants.REDIS_MAX_WAIT_MILLIS).getOrElse(3000).asInstanceOf[Int]
          )
        }
      }
    }
    this.pool
  }


  /**
    * 获取一个Jedis连接
    *
    * @return
    */
  def getConn(): Jedis = {
    val pool = getPool()
    pool.getResource
  }

  /**
    *
    * @param jedis
    */
  def closeConn(jedis: Jedis): Unit = {
    val pool = getPool()
    pool.returnResource(jedis)
  }

  /**
    * 关闭连接池
    */
  def close(): Unit = {
    if (this.pool != null) {
      this.pool.destroy()
      this.pool = null
    }
  }
}
