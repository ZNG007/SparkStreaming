package com.hbzq.bigdata.spark

import java.util.concurrent.{Executors, TimeUnit}

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.operator.runnable.{FlushRedisToMysqlTask, RedisDelKeyTask}
import com.hbzq.bigdata.spark.utils._
import org.apache.hadoop.hdfs.DistributedFileSystem
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

//    println(BigDecimal(0.0) == 0)
//    println(1.hashCode())
//      val str = "abc".getBytes()
//      println(new String(str))
//    println((Integer.MAX_VALUE-789).toString.reverse)

    import org.apache.hadoop.security.UserGroupInformation
    import java.security.PrivilegedAction
    import org.apache.hadoop.conf.Configuration

    val conf = new Configuration()

    val krb5_path = "E:\\javaProjects\\learn-bigdata\\learn-hive\\src\\main\\resources\\krb5.conf"
    val keytab_path = "E:\\javaProjects\\learn-bigdata\\learn-hive\\src\\main\\resources\\hive.keytab"

    System.setProperty("java.security.krb5.conf", krb5_path)

    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(conf)
    try
      UserGroupInformation.loginUserFromKeytab("hive/ut01.hbzq.com@HBZQ.COM", keytab_path)
    catch {
      case e: Exception =>

        e.printStackTrace()
    }

    UserGroupInformation.getCurrentUser.doAs(new PrivilegedAction[String]() {
      override def run: String = {
//        HBaseUtil.insertMessageToHBase("trade_monitor:tdrwt_wth","1111","info","channel","123")
        println(HBaseUtil.getMessageStrFromHBase("trade_monitor:tdrwt_wth","2222","info","channel"))
        ""

      }
    })

//    RedisDelKeyTask(DateUtil.getFormatNowDate()).run()
  }


}
