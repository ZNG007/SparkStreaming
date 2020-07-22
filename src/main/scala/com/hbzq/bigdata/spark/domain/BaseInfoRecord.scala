package com.hbzq.bigdata.spark.domain

/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
trait BaseInfoRecord extends BaseRecord{
}

// CUSTOMER.TKHXX  KHH, KHRQ, JGBZ
case class TkhxxRecord(var khh: String, var khrq: Int, var jgbz: String) extends BaseInfoRecord

// TDRZJMX KHH,LSH,YWKM,BZ,SRJE,FCJE
case class TdrzjmxRecord(var khh: String, var lsh: String,
                         ywkm: String, bz: String, op: String, je: BigDecimal
                        ) extends BaseInfoRecord

// TSSCJ   "KHH","CJBH","YYB","WTFS","WTGY","BZ","CJJE","S1","CXBZ","WTH"
case class TsscjRecord(var khh: String, var cjbh: String
                       , var yyb: String, var bz: String, var cjje: BigDecimal,
                       var s1: BigDecimal, var cxbz: String, var wth: String, var channel: String = "undefine"
                      ) extends BaseInfoRecord

case class DeadRecord() extends BaseRecord
