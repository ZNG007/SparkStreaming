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
case class NullRecord() extends BaseRecord

// CUSTOMER.TKHXX
case class TkhxxRecord(var khh: String, var khrq: Int, var jgbz: String) extends BaseRecord

// TDRZJMX
case class TdrzjmxRecord(var khh: String,
                         var lsh: String,
                         var ywkm: String,
                         var bz: String,
                         var op: String,
                         var je: BigDecimal) extends BaseRecord

// TSSCJ
case class TsscjRecord(var khh: String,
                       var cjbh: String,
                       var yyb: String,
                       var bz: String,
                       var cjje: BigDecimal,
                       var s1: BigDecimal,
                       var cxbz: String,
                       var wth: String,
                       var channel: String = "undefine",
                       var version: Int = 0) extends BaseRecord

case class SlowMessageRecord(var originTopic: String, var baseRecord: BaseRecord) extends BaseRecord
