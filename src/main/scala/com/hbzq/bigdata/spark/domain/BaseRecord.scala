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
case class TkhxxRecord(var khh: String,var khrq: Int, var jgbz: String, var khzt: String,var xhrq: String, var time: String,var op:String) extends BaseRecord

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
                       var gdh:String,
                       var channel: String = "undefine",
                       var version: Int = 0) extends BaseRecord

//OTC TFP_YWSQLS
case class TFPYwsqlsRecord(var op:String,
                           var rowid:String,
                           var wth:String,
                           var khh:String,
                           var ywdm:String,
                           var clzt:String,
                           var cljg:String,
                           var cpfl:String,
                           var ywje:BigDecimal,
                           var djrq:String,
                           var cpid:String,
                           var time:String,
                           var version: Int = 0
                          ) extends BaseRecord

case class TgdzhRecord(var khh: String, var gdh: String, var khrq: Int, var gdzt: String, var zhlb: String) extends BaseRecord
case class TzjzhRecord(var khh: String, var ZJZH: String, var khrq: Int, var zhzt: String ) extends  BaseRecord
case class TywqqRecord(var op:String,
                       var rowid:String,
                       var id: String,
                       var khh: String,
                       var sqrq: Int,
                       var yyb:String,
                       var clzt: String,
                       var time: String,
                       var version: Int = 0
                      ) extends  BaseRecord
//case class TywsqlsRecord(var wth: String, var djrq: Int, var ywdm:String, var clzt: String, var cljg:String) extends  BaseRecord
case class TjjzhRecord(var op:String,
                       var khh:String,
                       var jjzh:String,
                       var khrq: Int,
                       var zhzt: String
                      ) extends  BaseRecord

case class SlowMessageRecord(var originTopic: String, var baseRecord: BaseRecord) extends BaseRecord
