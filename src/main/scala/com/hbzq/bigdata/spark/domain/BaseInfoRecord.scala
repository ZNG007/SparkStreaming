package com.hbzq.bigdata.spark.domain

/**
  * describe:
  * create on 2020/06/01
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
trait BaseInfoRecord {
}

// CUSTOMER.TKHXX  KHH, KHRQ, JGBZ
case class TkhxxRecord(var khh: String, var khrq: Int, var jgbz: String) extends BaseInfoRecord

// TJGMXLS KHH,LSH,YYB,S1,S11,S12,S13
case class TjgmxlsRecord(var khh: String, var lsh: String, yyb: String,
                         s1: BigDecimal, s11: BigDecimal, s12: BigDecimal,
                         s13: BigDecimal) extends BaseInfoRecord {}

// TDRZJMX KHH,LSH,YWKM,BZ,SRJE,FCJE
case class TdrzjmxRecord(var khh: String, var lsh: String,
                         ywkm: String, bz: String, op: String, je: BigDecimal
                        ) extends BaseInfoRecord

