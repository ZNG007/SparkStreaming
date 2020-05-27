package com.hbzq.bigdata.spark.config

import java.io.{FileInputStream, IOException}
import java.util.Properties

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
    var in: FileInputStream = null
    try {
      in = new FileInputStream(fileName)
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
    }
  }
}
