package com.hbzq.bigdata.spark.domain

/**
  * describe:
  * create on 2020/07/21
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
trait BaseRecord {

}

/**
  * 空消息
  */
case class NullRecord() extends BaseRecord{}
