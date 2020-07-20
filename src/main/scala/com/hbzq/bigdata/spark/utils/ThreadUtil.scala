package com.hbzq.bigdata.spark.utils

import java.util.concurrent.atomic.AtomicInteger
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
    * @param threads 线程数
    * @return
    */
  def getSingleScheduleThreadPool(threads: Int): ScheduledExecutorService = {
    Executors.newScheduledThreadPool(threads, new ThreadFactory {
      val count = new AtomicInteger(0)
      override def newThread(runnable: Runnable): Thread = {
        val t = new Thread(runnable, s"Customer-Scheduler-Thread-${count.getAndIncrement()}");
        t.setDaemon(true)
        t
      }
    })
  }
}