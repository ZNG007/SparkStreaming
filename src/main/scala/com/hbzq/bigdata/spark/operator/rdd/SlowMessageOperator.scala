package com.hbzq.bigdata.spark.operator.rdd

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * describe:
  * create on 2020/07/20
  * 未关联消息处理
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
class SlowMessageOperator(var rdd: RDD[ConsumerRecord[String, String]]) extends RddOperator {
  override def compute(): Unit = {

    rdd
      .filter(message => {
        message.topic().equalsIgnoreCase(ConfigurationManager.getProperty(Constants.KAFKA_TOPIC_TRADE_MONITOR_SLOW_NAME))
      })
      .coalesce(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)





  }
}

object SlowMessageOperator {

  val logger = Logger.getLogger(SlowMessageOperator.getClass)

  def apply(rdd: RDD[ConsumerRecord[String, String]]): SlowMessageOperator = new SlowMessageOperator(rdd)
}


