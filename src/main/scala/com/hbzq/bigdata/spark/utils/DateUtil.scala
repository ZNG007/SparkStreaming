package com.hbzq.bigdata.spark.utils

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}

/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object DateUtil {
  val dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd")

  /**
    * 获取当前日期
    *
    * @return
    */
  def getFormatNowDate(): Int = {
    dateFormat.format(LocalDateTime.now()).toInt
  }

  /**
    * 获取当前时间戳
    *
    * @return
    */
  def getNowTimestamp(): Timestamp = {
    new Timestamp(LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli())
  }

  /**
    *
    * @return
    */
  def getDurationTime():Long ={
    val now = LocalDateTime.now()
    val nowStamp = now.toInstant(ZoneOffset.of("+8")).toEpochMilli()
    val stopHour = ConfigurationManager.getInt(Constants.APP_STOP_HOUR)
    val yaer = now.getYear
    val month = now.getMonth
    val day = now.getDayOfMonth

    val stopStamp = LocalDateTime.of(yaer,month,day,stopHour,0,0).toInstant(ZoneOffset.of("+8")).toEpochMilli()
    stopStamp - nowStamp
  }
}
