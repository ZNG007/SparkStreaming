package com.hbzq.bigdata.spark

import java.util.concurrent.{Executors, TimeUnit}

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.operator.runnable.{FlushRedisToMysqlTask, RedisDelKeyTask}
import com.hbzq.bigdata.spark.utils.{DateUtil, JsonUtil, RedisUtil, ThreadUtil}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._


/**
  * describe:
  * create on 2020/05/29
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object Test {
  def main(args: Array[String]): Unit = {
//    ConfigurationManager.initConfig("E:\\scalaProjects\\realtime-trade-monitor\\src\\main\\resources\\config.properties")
    // 获取Redis配置
//    val redisConf = Map(
//      Constants.REDIS_HOSTS -> ConfigurationManager.getProperty(Constants.REDIS_HOSTS).split(Constants.DELIMITER).toSet,
//      Constants.REDIS_MASTER -> ConfigurationManager.getProperty(Constants.REDIS_MASTER),
//      Constants.REDIS_TIMEOUT -> ConfigurationManager.getInt(Constants.REDIS_TIMEOUT),
//      Constants.REDIS_MAX_TOTAL -> ConfigurationManager.getInt(Constants.REDIS_MAX_TOTAL),
//      Constants.REDIS_MAX_IDLE -> ConfigurationManager.getInt(Constants.REDIS_MAX_IDLE),
//      Constants.REDIS_MIN_IDLE -> ConfigurationManager.getInt(Constants.REDIS_MIN_IDLE),
//      Constants.REDIS_TEST_ON_BORROW -> ConfigurationManager.getBoolean(Constants.REDIS_TEST_ON_BORROW),
//      Constants.REDIS_TEST_ON_RETURN -> ConfigurationManager.getBoolean(Constants.REDIS_TEST_ON_RETURN),
//      Constants.REDIS_MAX_WAIT_MILLIS -> ConfigurationManager.getInt(Constants.REDIS_MAX_WAIT_MILLIS)
//    )

//        RedisUtil.init(redisConf)
//    val jedis = new Jedis("10.105.1.161", 6379)
//    val keys = RedisUtil.getConn().keys("*")
//    print(keys)
//    val pp = jedis.pipelined()
//    keys.asScala.foreach(pp.get(_))
//    print(pp.syncAndReturnAll())
//    print(BigDecimal(10003)/BigDecimal(100))
//    var res: List[List[Any]] = List()
//    res ::= (1 :: 2 :: 3 :: 4 :: 5 :: 6 :: 7 :: 8 :: Nil)
//    res ::= (1 :: 2 :: 3 :: 4 :: 5 :: 6 :: 7 :: 8 :: Nil)
//
//    print(res)


    //    print(jedis.incr("trade_monitor_tdrwt_kh_count"))
    //    RedisUtil.getConn.setbit("trade_monitor_tdrwt_khh","111111111111".toLong,true)
    //    print(BigDecimal("100.00").toDouble*100.toDouble)

//    var a:Set[(String,String)] = Set(("1","2"))
//    a += (("1","2"))
//    print(a)
//    import scala.collection.mutable.{Set, Map}
//    var tempKhhsMap:Map[String,Set[String]] = Map()
//    if (!tempKhhsMap.keySet.contains("1")) tempKhhsMap += ("1" -> Set())
//
//    var s = tempKhhsMap.getOrElse("1",Set())
//     s += ("1")
//
//    print(tempKhhsMap)

//    startScheduleTask()
//    new RedisDelKeyTask().run()
//    new FlushRedisToMysqlTask().run()

//    println(DateUtil.getDurationTime())

//    print(Array((1,2),(3,4)).toMap)
//    print("01110_1".split("_")(1))

    println(BigDecimal(0.0) == 0)
  }

  /**
    * 调度任务
    *
    * @return
    */
  private def startScheduleTask() = {
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
  }
}
