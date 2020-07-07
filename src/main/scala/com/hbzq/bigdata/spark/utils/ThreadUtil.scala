package com.hbzq.bigdata.spark.utils

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, ThreadPoolExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.util.UninterruptibleThread

/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object ThreadUtil {

  /**
    * 获取调度线程池
    *
    * @param i
    * @return
    */
  def getSingleScheduleThreadPool(i:Int): ScheduledExecutorService = {

    Executors.newScheduledThreadPool(i)
  }
}