package com.hbzq.bigdata.spark.operator.record

import java.io.{File, PrintWriter}
import java.sql.{DriverManager, ResultSet}

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.domain._
import com.hbzq.bigdata.spark.operator.rdd.{TdrwtOperator, TkhxxOperator, TsscjOperator}
import com.hbzq.bigdata.spark.utils._
import org.apache.hadoop.hbase.client.Put
import scalikejdbc.SQL
//import com.hbzq.bigdata.spark.utils.
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.util.Random

/**
  * describe:
  * create on 2020/07/24
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */
object BaseRecordOperator {
  private[this] val logger = Logger.getLogger(BaseRecordOperator.getClass)

  /**
    * 将Kafk中的消息转化成BaseRecord
    *
    * @param records
    * @return
    */
  def  parsePartitionKafkaRecordsToBaseRecord(records: Iterator[ConsumerRecord[String, String]]): Iterator[(String, BaseRecord)] = {
//    logger.warn("topic********************")
    var res: List[(String, BaseRecord)] = List()
    records.foreach(record => {
    //  logger.warn("topic********************"+record.topic())
      record.topic().toUpperCase match {
        // 新开户
        case "CIF_TKHXX" => {
         // logger.warn("****************CIF_TKHXX************************")
          val tkhxxRecord = JsonUtilV2.parseKakfaRecordToTkhxxRecord(record.value())
          if (tkhxxRecord != null &&
            !"".equalsIgnoreCase(tkhxxRecord.khh)
         //  &&  !TkhxxOperator.QT.equalsIgnoreCase(tkhxxRecord.jgbz)
          ) {
            res ::= (Random.nextInt(100000).toString+"6", tkhxxRecord)
          }
        }
        // 实时成交
        case "SECURITIES_TSSCJ" => {
          val tsscjRecord = JsonUtilV2.parseKakfaRecordToTsscjRecord(record.value())
          if (tsscjRecord != null &&
            !"".equalsIgnoreCase(tsscjRecord.khh) &&
            !"0".equalsIgnoreCase(tsscjRecord.wth) &&
            !"".equalsIgnoreCase(tsscjRecord.cjbh) &&
            "O".equalsIgnoreCase(tsscjRecord.cxbz)) {
            res ::= (tsscjRecord.wth, tsscjRecord)
          }
        }
        // 当日资金明细
        case "ACCOUNT_TDRZJMX" => {
          val tdrzjmxRecord = JsonUtilV2.parseKakfaRecordToTdrzjmxRecord(record.value())
          if (tdrzjmxRecord != null &&
            !"".equalsIgnoreCase(tdrzjmxRecord.khh) &&
            !"".equalsIgnoreCase(tdrzjmxRecord.lsh)) {
            res ::= (Random.nextInt(100000).toString+"5", tdrzjmxRecord)
          }

        }
        // 当日委托
        case "SECURITIES_TDRWT" => {
          val tdrwtRecord = JsonUtilV2.parseKakfaRecordToTdrwtRecord(record.value())
          if (tdrwtRecord != null &&
            !"0".equalsIgnoreCase(tdrwtRecord.wth)) {
            res ::= (tdrwtRecord.wth, tdrwtRecord)
          }
        }


        //          2020/10/20新增OTC FPSS.TFP_YWSQLS
//        case class TFPYwsqlsRecord(var op:String,
//                                   var wth:String,
//                                   var khh:String,
//                                   var ywdm:String,
//                                   var clzt:String,
//                                   var cljg:String,
//                                   var cpfl:String,
//                                   var ywje:BigDecimal,
//                                   var djrq:String,
//                                   var cpid:String
//                                  ) extends BaseRecord

        case "FPSS_TFP_YWSQLS" =>{
         // logger.warn("==================otc2========================")
          val tfpywsqlsRecord = JsonUtilV2.parseKafkaRecordToTFPYwsqlsRecord(record.value())
          if(tfpywsqlsRecord != null &&
            !"0".equalsIgnoreCase(tfpywsqlsRecord.wth)
          //  && tfpywsqlsRecord.time==DateUtil.getFormatNowDate().toString
            && tfpywsqlsRecord.djrq==DateUtil.getFormatNowDate().toString
          ){
//            logger.warn("rowid"+tfpywsqlsRecord.rowid+"op: "+tfpywsqlsRecord.op+"======="+tfpywsqlsRecord.clzt+"wth："+tfpywsqlsRecord.wth+" "+tfpywsqlsRecord.cljg+
//              tfpywsqlsRecord.cpfl+tfpywsqlsRecord.cpid+"ywdm"+tfpywsqlsRecord.ywdm+tfpywsqlsRecord.djrq+"+==================otc3========================"+tfpywsqlsRecord)
            res ::=(Random.nextInt(100000).toString+"4",tfpywsqlsRecord)
          }
        }
        //股东帐户开户数
        case "CIF_TGDZH" =>{
          val tgdzhRecord=JsonUtilV2.parseKafkaRecordToTgdzhRecord(record.value())
          //    logger.info("****************tgdzhRecord************************")
          //    logger.warn("*****判断逻辑********"+tgdzhRecord.khh+"  "+"  "+tgdzhRecord.gdh+"  "+tgdzhRecord.gdzt+"  "+tgdzhRecord.khrq)
          //println(tgdzhRecord.khh+"**********"+tgdzhRecord.gdzt")
          if (tgdzhRecord != null &&
            !"".equalsIgnoreCase(tgdzhRecord.khh)&&
            tgdzhRecord.khrq==DateUtil.getFormatNowDate()
            && "0"==tgdzhRecord.gdzt
          ) {
            // println("**************"+tgdzhRecord.khh+tgdzhRecord.khrq)
            //      logger.warn("**************"+tgdzhRecord.gdh)
            res ::= (tgdzhRecord.gdh, tgdzhRecord)
          }
        }

        case "CIF_TZJZH" =>{
          val TzjzhRecord=JsonUtilV2.parseKafkaRecordToTzjzhRecord(record.value())
          //          logger.info("****************tgdzhRecord************************")
          //          logger.warn("*****判断逻辑********"+TzjzhRecord.khh+"  ")
          //println(tgdzhRecord.khh+"**********"+tgdzhRecord.gdzt")
          if (TzjzhRecord != null &&
            !"".equalsIgnoreCase(TzjzhRecord.khh)&&
            TzjzhRecord.khrq==DateUtil.getFormatNowDate()
          ) {
            // println("**************"+tgdzhRecord.khh+tgdzhRecord.khrq)
            //  logger.warn("**************"+TzjzhRecord.ZJZH)
            res ::= (TzjzhRecord.ZJZH, TzjzhRecord)
          }
        }
        case "CIF_TYWQQ" =>{
        //  logger.warn("*****CIF_TYWQQ*****")
          val TywqqRecord=JsonUtilV2.parseKafkaRecordToTywqqRecord(record.value())
          if (TywqqRecord != null &&
            !"nvl".equalsIgnoreCase(TywqqRecord.id)&&
            TywqqRecord.time==DateUtil.getFormatNowDate().toString &&
//            "8".equalsIgnoreCase(TywqqRecord.clzt) &&
            !"nvl".equalsIgnoreCase(TywqqRecord.rowid)
          ) {
            res ::= (Random.nextInt(100000).toString+"3", TywqqRecord)
          }
        }
        case "OFS_TOF_JJZH" =>{
        //  logger.warn("*****CIF_TYWQQ*****")
          val TjjzhRecord=JsonUtilV2.parseKafkaRecordToTjjzhRecord(record.value())
          if (TjjzhRecord != null
            && !"nvl".equalsIgnoreCase(TjjzhRecord.khh)
            && TjjzhRecord.khrq==DateUtil.getFormatNowDate()
            && "8".equalsIgnoreCase(TjjzhRecord.zhzt)
          ) {
            logger.warn("jjzh2 "+TjjzhRecord)
            res ::= (Random.nextInt(100000).toString+"7", TjjzhRecord)
          }
        }




        /**
          * 先到但未关联到HBase中的数据的消息，重新将消息发回Kafka,并增加消息的版本信息,
          * 默认如果被反复处理固定的次数，将不再关联,并按照未关联的默认值输出
          * 目前针对的是Tsscj未关联到的消息
          */
        case "TRADE_MONITOR_SLOW" => {
          val slowMessageRecord = JsonUtilV2.parseKakfaRecordToSlowMessageRecord(record.value())
          slowMessageRecord.originTopic match {
            case "SECURITIES_TSSCJ" => {
              val tsscjRecord = slowMessageRecord.baseRecord.asInstanceOf[TsscjRecord]
              if (tsscjRecord != null &&
                !"".equalsIgnoreCase(tsscjRecord.khh)
                && !"0".equalsIgnoreCase(tsscjRecord.wth) &&
                !"".equalsIgnoreCase(tsscjRecord.cjbh) &&
                "O".equalsIgnoreCase(tsscjRecord.cxbz)) {
                res ::= (tsscjRecord.wth, tsscjRecord)
              }
            }
            case "SECURITIES_TYWQQ" => {
              val tywqqRecord = slowMessageRecord.baseRecord.asInstanceOf[TywqqRecord]
              logger.warn("slowywqq "+tywqqRecord)
              if (tywqqRecord != null &&
                !"nvl".equalsIgnoreCase(tywqqRecord.id)
                && tywqqRecord.time==DateUtil.getFormatNowDate().toString
                ) {
                res ::= (Random.nextInt(10000).toString+"2", tywqqRecord)
              }
            }
            case "SECURITIES_TYWSQLSKH" => {
              val tfpywsqlsRecord = slowMessageRecord.baseRecord.asInstanceOf[TFPYwsqlsRecord]
              logger.warn("slowywsqls "+tfpywsqlsRecord)
              if (tfpywsqlsRecord != null &&
                !"0".equalsIgnoreCase(tfpywsqlsRecord.wth)
                && tfpywsqlsRecord.time==DateUtil.getFormatNowDate().toString
              ) {
                res ::= (Random.nextInt(10000).toString+"1", tfpywsqlsRecord)
              }
            }
            case _ =>
          }
        }
        case _=>
          logger.warn(record+"=========未匹配========")
      }
    })
    res.iterator
  }


  /**
    * 分区数据聚合操作
    *
    * @param records
    * @param exchangeMapBC
    * @param tfpCpdmMapBC add 20201028 by wf
    * @param kafkaProducerBC
    * @return
    */
  def aggregatePartitionBaseRecords(records: Iterator[(String, Iterable[BaseRecord])],
                                    exchangeMapBC: Broadcast[Map[String, BigDecimal]],
                                    tfpCpdmMapBC:Broadcast[Map[String,(String,String)]],
                                    kafkaProducerBC: Broadcast[KafkaSink[String, String]]): Iterator[mutable.Map[String, Any]] = {
    import scala.collection.mutable.Map


    // 结果集
   // logger.warn("**********************************************************************")
    var res: mutable.Map[String, Any] = Map()
    // 委托插入及更新集合
    val tdrwtInsertRecords: mutable.Map[String, TdrwtRecord] = Map()
    val tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord] = Map()
    //    OTC 业务申请流水 20201021
    val tFPYwsqlsInsertRecords:mutable.Map[String,TFPYwsqlsRecord] = Map()
    val tFPYwsqlsUpdateRecords:mutable.Map[String,TFPYwsqlsRecord] = Map()
    val tFPYwsqlsRecords:mutable.Map[String,TFPYwsqlsRecord] = Map()

    val tywqq: mutable.Map[Int, TywqqRecord] = Map()
    val tywqqInsertRecords: mutable.Map[String,TywqqRecord]=Map()
    val tywqqUpdateRecords: mutable.Map[String,TywqqRecord]=Map()
    // 成交集合
    val tsscjRecords: mutable.Map[String, TsscjRecord] = Map()
    // 未关联集合
    var slowMessageList: List[SlowMessageRecord] = List()
    // 成交，委托，新开户，资金 结果集 20201021新增OTC业务申请流水
    val tdrwt: mutable.Map[(String, String), (Int,Int,Int,Int,Int,BigDecimal)] = Map()
    val tkhxx: mutable.Map[String, Int] = Map()
    val tkhxxInsertRecords:mutable.Map[String,TkhxxRecord]=Map()
    val tkhxxUpdateRecords:mutable.Map[String,TkhxxRecord]=Map()
    val tdrzjmx: mutable.Map[String, BigDecimal] = Map()
    val tsscj: mutable.Map[(String, String), (Int, BigDecimal,BigDecimal,BigDecimal)] = Map()
    val tfpywsqls:mutable.Map[String,(Int,BigDecimal)] = Map()
    val tfpywsqlscount:mutable.Map[String,(Int,BigDecimal)] = Map()
   // if(!"".equals(tFPYwsqlsInsertRecords)){logger.warn(tFPYwsqlsInsertRecords+"================解析 5===============")}

    //股东账户开户数
    val tgdzh: mutable.Map[String, Int] = Map()
    val tjjzh: mutable.Map[String, Int] = Map()
    val tzjzh: mutable.Map[String, Int] = Map()
    val txhzh: mutable.Map[String, Int] = Map()
    val tywqqrs: mutable.Map[String, Int] = Map()
    val  tup : mutable.Map[String,(String ,String , BigDecimal)]=Map()



    // 首先遍历一遍  针对不同的消息进行处理
    classifyAndComputeBaseRecord(records, tdrwtInsertRecords, tdrwtUpdateRecords, tsscjRecords, tkhxx, tkhxxInsertRecords, tkhxxUpdateRecords, txhzh, tdrzjmx, tFPYwsqlsRecords,tFPYwsqlsInsertRecords,tFPYwsqlsUpdateRecords,tgdzh, tjjzh, tzjzh, tywqq,tywqqInsertRecords,tywqqUpdateRecords,exchangeMapBC)
    //  TDRWT Insert 批量插入HBase
    putTdrwtRecordsToHBase(tdrwtInsertRecords)
   // putTkhxxRecordsToHBase(tkhxxInsertRecords)
    putTywqqRecordsToHBase(tywqqInsertRecords)
    putTywsqlsRecordsToHBase(tFPYwsqlsInsertRecords)
    // 计算OTC指标  20201028 add by wf TODO
    computeTfpywsqlsRecord(tFPYwsqlsInsertRecords,tfpCpdmMapBC,tfpywsqls)
//    computeTfpywsqlsRecordcount(tFPYwsqlsRecords,tfpywsqlscount)
//    computeTywqq(tywqq,tywqqrs)



    var jedis: Jedis = null
    try {
      jedis = RedisUtil.getConn()
      val (tdrwtMixWths, tsscjMixWths, hbaseTdrwtReords) = getAllTdrwtFromHBaseOrPartition(tdrwtInsertRecords, tdrwtUpdateRecords, tsscjRecords)
   // 增加Insert消息里面  sbjg=2
      computeTdrwtInsertRecord(tdrwtInsertRecords, exchangeMapBC, tdrwt, jedis)
   // 计算Tsscj
      slowMessageList :::= computeTsscjRecord(tsscjRecords, tdrwtInsertRecords, tsscjMixWths, hbaseTdrwtReords, exchangeMapBC, tsscj, jedis,tup)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      RedisUtil.closeConn(jedis)
    }
    val  hbaseTkhxxReords= getAllTkhxxFromHBaseOrPartition(tkhxxInsertRecords, tkhxxUpdateRecords)
    val  hbaseTywqqReords= getAllTywqq1FromHBaseOrPartition(tywqqInsertRecords, tywqqUpdateRecords)
    val  hbaseTywsqlsReords = getAllTywsqls1FromHBaseOrPartition(tFPYwsqlsInsertRecords, tFPYwsqlsUpdateRecords)
    //computeTkhxxInsertRecord(tkhxxInsertRecords, txhzh)
    computeTywqqInsertRecord(tywqqInsertRecords, tywqqrs)
    computeTywsqlsInsertRecord(tFPYwsqlsInsertRecords, tfpywsqlscount)

    // 计算tdrwt相关指标 暂不考虑Update
   // computeTdrwtUpdateRecord(tdrwtUpdateRecords, tdrwtInsertRecords, tdrwtMixWths, hbaseTdrwtReords, exchangeMapBC, tdrwt, jedis)
    //slowMessageList :::= computeTkhxxUpdateRecord(tkhxxUpdateRecords, tkhxxInsertRecords,hbaseTkhxxReords,txhzh)

    slowMessageList :::= computeTywqqUpdateRecord(tywqqUpdateRecords, tywqqInsertRecords,hbaseTywqqReords,tywqqrs)

    //computeTywqqUpdateRecord(tywqqUpdateRecords, tywqqInsertRecords,tywqqrs)
    slowMessageList :::= computeTywsqlsUpdateRecord(tFPYwsqlsUpdateRecords, tFPYwsqlsInsertRecords,hbaseTywsqlsReords,tfpywsqlscount,tfpywsqls)

    // 往Kafka 发送未关联消息
    pushMassageToKafka(slowMessageList, kafkaProducerBC)
    res += ("tdrwt" -> tdrwt)
    res += ("tsscj" -> tsscj)
    res += ("tdrzjmx" -> tdrzjmx)
    res += ("tkhxx" -> tkhxx)
    res += ("tfpywsqls" -> tfpywsqls)
    res += ("tfpywsqlscount" -> tfpywsqlscount)
    res +=("tgdzh" -> tgdzh)
    res +=("txhzh" -> txhzh)
    res +=("tjjzh" -> tjjzh)
    res +=("tzjzh" -> tzjzh)
    res +=("tywqq" -> tywqqrs)
    List(res).iterator
  }

  /**
    * 推送消息到Kafka
    *
    * @param slowMessageList
    * @param kafkaProducerBC
    */
  private def pushMassageToKafka(slowMessageList: List[SlowMessageRecord], kafkaProducerBC: Broadcast[KafkaSink[String, String]]) = {
    slowMessageList
      .map(message => JsonUtilV2.parseObjectToJson(message))
      .foreach(message => {
        kafkaProducerBC.value.send(ConfigurationManager.getProperty(Constants.KAFKA_TOPIC_TRADE_MONITOR_SLOW_NAME), message)
      })
  }

  /**
    * 计算 tsscj 指标
    *
    * @param tsscjRecords
    * @param tdrwtInsertRecords
    * @param tsscjMixWths
    * @param hbaseTdrwtReords
    * @param exchangeMapBC
    * @param tsscj
    * @param jedis
    */
  def computeTsscjRecord(tsscjRecords: mutable.Map[String, TsscjRecord],
                         tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                         tsscjMixWths: collection.Set[String],
                         hbaseTdrwtReords: mutable.Map[String, mutable.Map[String, String]],
                         exchangeMapBC: Broadcast[Map[String, BigDecimal]],
                         tsscj: mutable.Map[(String, String), (Int, BigDecimal, BigDecimal,BigDecimal)],
                         jedis: Jedis,
                         tup : mutable.Map[String,(String ,String , BigDecimal)]
                        ): List[SlowMessageRecord] = {
    var slowMessageList: List[SlowMessageRecord] = List()
    tsscjRecords.foreach(entry => {

      // key ： wth_cjbh
      val wth = entry._1.split("_")(0)
      val tsscjRecord = entry._2
      var channel: String = tsscjRecord.channel
      var jslx = ""
      var cxbz=""
      var flag = true
      if (tsscjMixWths.contains(wth)) {
        val tdrwtDetail = tdrwtInsertRecords.get(wth).get
        channel = tdrwtDetail.channel
        jslx = tdrwtDetail.jslx
        cxbz=tdrwtDetail.cxbz
      } else {
        // HBase中获取
        if (hbaseTdrwtReords.keySet.contains(wth)) {
          val data = hbaseTdrwtReords.get(wth).get
          channel = data.get("CHANNEL").getOrElse("undefine")
        } else {
          // 关联不到的记录处理

          val threshold = ConfigurationManager.getInt(Constants.TSSCJ_LIFE_CYCLE_THRESHOLD)
          val version = tsscjRecord.version
          flag = version match {
            case version if (version < threshold) => {
              tsscjRecord.version = version + 1
              val slowMessage = SlowMessageRecord("SECURITIES_TSSCJ", tsscjRecord)
              slowMessageList ::= slowMessage
              false
            }
            case _ => {
              logger.error(
                s"""
                   |==================
                   |over threshold : ${threshold}
                   |can't get channel from HBase, use defalut value to compute....
                   |$tsscjRecord
                   |==================
                  """.stripMargin)
              true
            }
          }
        }
      }
      if (flag) {
        val bz = tsscjRecord.bz.toUpperCase
        val khh = tsscjRecord.khh
        val yyb = tsscjRecord.yyb
        val jyj = tsscjRecord.s1
        val gdh = tsscjRecord.gdh
        // 更新Redis
        if (!"".equalsIgnoreCase(khh) && khh.length > 4) {
          val tempKhh = khh.substring(4).toInt
          jedis.setbit(s"${TsscjOperator.TSSCJ_KHH_PREFIX}${DateUtil.getFormatNowDate()}_${yyb}_$channel", tempKhh, true)
        } else {
          logger.error(
            s"""
               |===================
               |TSSCJ Operator khh is invaild....
               |$khh
               |===================
              """.stripMargin)
        }
        val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
        val cjje = tsscjRecord.cjje * exchange
        var cjje_marg=BigDecimal(0)
//        logger.warn(gdh+"======================gdh2===========================")
//        logger.warn(cjje_marg+"======================cjje_marg2===========================")
        if(
//          cxbz.equalsIgnoreCase("O") &&
            (gdh.startsWith("E")||gdh.startsWith("06"))
        ){

          cjje_marg=tsscjRecord.cjje * exchange
//          logger.warn(gdh+"======================gdh===========================")
//          logger.warn(cjje_marg+"======================cjje_marg===========================")
         // putTcjjeRecordsToHBase(wth,gdh,cjje,tup)

//          var otherStates: List[List[Any]] = List()
//          otherStates ::= (gdh :: cjje_marg :: Nil)
//          val sql="INSERT INTO dm.dm_marg (gdh, cjje) VALUES (?, ?)"
//          MysqlJdbcUtil.executeBatchUpdate(sql,otherStates)
//          var otherStates: List[List[Any]] = List()
//          otherStates ::= (gdh :: cjje_marg :: Nil)
//          otherStates.foreach(otherState => {
//            SQL("INSERT INTO dm.dm_marg Values(?,?)")
//              .bind(otherState: _*).update().apply()
////          })
//          val writer = new PrintWriter(new File("includehelp.txt"))
//          writer.write("Scala is an amazing programming language")
//          writer.close()
/*
          val dbc = "jdbc:mysql://10.104.10.30:3306/dm?user=root&password=hbzq0817A"
          classOf[com.mysql.jdbc.Driver]
          val conn = DriverManager.getConnection(dbc)
          val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)

          // do database insert
          try {
            val prep = conn.prepareStatement("INSERT INTO dm.dm_marg (gdh, cjje) VALUES (?, ?) ")
            prep.setString(1,gdh)
            prep.setBigDecimal(2, cjje_marg.bigDecimal)
            prep.executeUpdate
          }
          finally {
            conn.close
          }
*/

        }

        val oldValue = tsscj.getOrElse((yyb, channel), (0, BigDecimal(0), BigDecimal(0),BigDecimal(0)))
        tsscj.put((yyb, channel), (oldValue._1 + 1, oldValue._2 + cjje, oldValue._3 + jyj,oldValue._4+cjje_marg))
      }
    })
    slowMessageList
  }

  /*
  * TODO
  *
  * 计算tfpywsqls相关指标（Insert） add 20201026 wf
  * @param tFPYwsqlsInsertRecords
  * @param tfpCpdmMapBC
  *
  * */
  private def computeTfpywsqlsRecord(tFPYwsqlsInsertRecords:mutable.Map[String,TFPYwsqlsRecord],
                                     tfpCpdmMapBC:Broadcast[Map[String,(String,String)]],
                                     tfpywsqls:mutable.Map[String,(Int,BigDecimal)]
                                    )={
    tFPYwsqlsInsertRecords.foreach(record=> {
//      logger.warn("========================tfpywsqls 计算 ==================================")
      val tFPYwsqlsDetail = record._2
      val cpid= tFPYwsqlsDetail.cpid
      val ywdm =tFPYwsqlsDetail.ywdm
      val clzt = tFPYwsqlsDetail.clzt
      val cljg = tFPYwsqlsDetail.cljg
      val cpfl = tFPYwsqlsDetail.cpfl
      val ywje = tFPYwsqlsDetail.ywje
      val cpdmDetail=tfpCpdmMapBC.value.get(cpid).toMap
    //  logger.warn(cpid+"=================CPID OUT**************************")
      if(!"nvl".equalsIgnoreCase(cpid)){
     //   logger.warn(cpid+"=================CPID IN**************************")
     //   logger.warn(cpdmDetail.keys.head+"======================cpdmDetail**************************")
    //    logger.warn(cpdmDetail.values.head+"=======================cpdmDetail**************************")
        if("22".equalsIgnoreCase(ywdm) && "0".equalsIgnoreCase(clzt)  && cpfl.equals("5") && cpdmDetail.keys.head.equals("1000056") && cpdmDetail.values.head.equals("511")){
          val oldvalue = tfpywsqls.getOrElse("crrc",(0,BigDecimal(0)))
          tfpywsqls.put("crrc",(oldvalue._1+1,oldvalue._2+ywje))
        }
        if("22".equalsIgnoreCase(ywdm) && "0".equalsIgnoreCase(clzt)  && cpfl.equals("5") && cpdmDetail.keys.head.equals("1000056") && cpdmDetail.values.head.equals("509")){
          val oldvalue = tfpywsqls.getOrElse("not_crrc",(0,BigDecimal(0)))
          tfpywsqls.put("not_crrc",(oldvalue._1+1,oldvalue._2+ywje))
        }
      }
//      logger.warn(tfpCpdmMapBC.value+"=================tfpCpdmMapBC**************************")
//      logger.warn(cpid+"=================CPID**************************")
//      logger.warn(tfpCpdmMapBC.value.get(cpid)+"=================CPID2**************************")
//      logger.warn(cpdmDetail+"=================CPDM**************************")
//      logger.warn(ywdm+"=================YWDM**************************")
//      logger.warn(clzt+"=================CLZT**************************")

      //    val tcpdm:mutable.Map[String,(String,String)]=(cpid,cpdmDetail.)

      if(!"001".equalsIgnoreCase(ywdm) && "2".equalsIgnoreCase(clzt) && cljg.equals("1") && cpfl.equals("14")){
        val oldvalue = tfpywsqls.getOrElse("ins",(0,BigDecimal(0)))
        tfpywsqls.put("ins",(oldvalue._1+1,oldvalue._2+ywje))
      }



    }
    )

  }
  private def computeTfpywsqlsRecordcount(tFPYwsqlsRecords:mutable.Map[String,TFPYwsqlsRecord],
                                     tfpywsqlscount:mutable.Map[String,(Int,BigDecimal)]
                                    )={
    tFPYwsqlsRecords.foreach(record=> {

      val tFPYwsqlsDetail = record._2
      val op=tFPYwsqlsDetail.op
      val cpid= tFPYwsqlsDetail.cpid
      val ywdm =tFPYwsqlsDetail.ywdm
      val clzt = tFPYwsqlsDetail.clzt
      val cljg = tFPYwsqlsDetail.cljg
      val cpfl = tFPYwsqlsDetail.cpfl
      val ywje = tFPYwsqlsDetail.ywje
      val rowid = tFPYwsqlsDetail.rowid
    //  logger.warn("op: "+op+"rowid "+rowid+"ywdm："+ywdm+"cljg："+cljg+"clzt："+clzt+"ywsqls")
      if("2".equalsIgnoreCase(clzt)){
        val oldvalue = tfpywsqlscount.getOrElse("count",(0,BigDecimal(0)))
     //   logger.warn("otc4*******"+oldvalue._1+"   "+oldvalue._2 +"rowid "+rowid )
        tfpywsqlscount.put("count",(oldvalue._1+1,BigDecimal(0)))
      }
    }
    )

  }

  /**
    * 计算tdrwt相关指标(insert)
    *
    * @param tdrwtInsertRecords
    * @param exchangeMapBC
    * @param tdrwt
    * @param jedis
    */
  private def  computeTdrwtInsertRecord(
                                        tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                                        exchangeMapBC: Broadcast[Map[String, BigDecimal]],
                                        tdrwt: mutable.Map[(String, String), (Int,Int,Int,Int,Int,BigDecimal)],
                                        jedis: Jedis) = {
    tdrwtInsertRecords
      // One insert message  map one wt record
      //      .filter(record => "2".equalsIgnoreCase(record._2.sbjg))
      .foreach(record => {
      val tdrwtDetail = record._2
      val khh = tdrwtDetail.khh
      val wtjg = tdrwtDetail.wtjg
      val cxbz=tdrwtDetail.cxbz //add 202010 wf
      val jslx=tdrwtDetail.jslx // add 202010 wf
      val channel = tdrwtDetail.channel
      val yyb = tdrwtDetail.yyb
      val wtsl = tdrwtDetail.wtsl
      val bz = tdrwtDetail.bz
      val zqlb=tdrwtDetail.zqlb
      if (!"".equalsIgnoreCase(khh) && khh.length > 4) {
        val tempKhh = khh.substring(4).toInt
        // 更新Redis
        jedis.setbit(s"${TdrwtOperator.TDRWT_KHH_PREFIX}${DateUtil.getFormatNowDate()}_${yyb}_$channel", tempKhh, true)
      } else {
        logger.error(
          s"""
             |===================
             |TdrwtInsert Operator khh is invaild....
             |khh: $khh
             |===================
            """.stripMargin)
      }
      val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
      val wtje = wtsl * wtjg * exchange
      val oldValue = tdrwt.getOrElse((yyb, channel), (0,0,0,0,0,BigDecimal(0)))
      var wtbs_marg =0
      var wtbs_stoc =0
      var wtbs_bond =0
      var wtbs_fund =0
 //     var wtbs_warr =0
//       logger.warn(tdrwtDetail+"================TDRWT================")
//      logger.warn(khh+"=============KHH==========")
//      logger.warn(jslx+"=============JSLX**********")
//      logger.warn(cxbz+"=================CXBZ**********")
//      logger.warn("O".equals(cxbz)+"==================条件****************")
//      logger.warn("5".equals(jslx)+"===============条件****************")
      if("O".equals(cxbz) &&  "5".equals(jslx)){
        wtbs_marg = wtbs_marg+1
 //       logger.warn(wtbs_marg+"************笔数+1**********")
      }
      if((zqlb.startsWith("A")&& !"A9".equalsIgnoreCase(zqlb))||zqlb.startsWith("F")||zqlb.startsWith("C")||
        zqlb.startsWith("R")||zqlb.startsWith("N")||zqlb.equalsIgnoreCase("G0")){
        wtbs_stoc = wtbs_stoc+1
      }
      if(zqlb.startsWith("E")||zqlb.startsWith("J")||zqlb.startsWith("L")||zqlb.startsWith("T")){
          wtbs_fund=wtbs_fund+1
      }
      if(zqlb.startsWith("H")||zqlb.startsWith("Z")||zqlb.equalsIgnoreCase("A9")){
        wtbs_bond=wtbs_bond+1
      }

      tdrwt.put((yyb, channel), (oldValue._1 + 1,oldValue._2+wtbs_marg,oldValue._3+wtbs_stoc,oldValue._4+wtbs_fund,oldValue._5+wtbs_bond,oldValue._6 + wtje))
    })
  }


  /**
    * 计算tdrwt相关指标(update)
    *
    * @param tdrwtUpdateRecords
    * @param tdrwtInsertRecords
    * @param tdrwtMixWths
    * @param hbaseTdrwtReords
    * @param exchangeMapBC
    * @param tdrwt
    * @param jedis
    */
  private def computeTdrwtUpdateRecord(tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord],
                                       tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                                       tdrwtMixWths: collection.Set[String],
                                       hbaseTdrwtReords: mutable.Map[String, mutable.Map[String, String]],
                                       exchangeMapBC: Broadcast[Map[String, BigDecimal]],
                                       tdrwt: mutable.Map[(String, String), (Int, BigDecimal)],
                                       jedis: Jedis) = {
    tdrwtUpdateRecords.foreach(entry => {
      val wth = entry._1
      val tdrwtRecord = entry._2
      var khh: String = null
      var wtjg: BigDecimal = null
      var channel: String = null
      var yyb: String = null
      var wtsl: Int = 0
      var bz: String = null
      if (tdrwtMixWths.contains(wth)) {
        val tdrwtDetail = tdrwtInsertRecords.get(wth).get
        khh = tdrwtDetail.khh
        wtjg = tdrwtDetail.wtjg
        channel = tdrwtDetail.channel
        yyb = tdrwtDetail.yyb
        wtsl = tdrwtDetail.wtsl
        bz = tdrwtDetail.bz
      } else {
        // HBase中获取
        val data = hbaseTdrwtReords.get(wth).get
        khh = data.get("KHH").getOrElse("")
        yyb = data.get("YYB").getOrElse("")
        bz = data.get("BZ").getOrElse("")
        wtsl = data.get("WTSL").getOrElse("0").toInt
        wtjg = BigDecimal(data.get("WTJG").getOrElse("0"))
        channel = data.get("CHANNEL").getOrElse("qt")
      }
      if (!"".equalsIgnoreCase(khh) && khh.length > 4) {
        val tempKhh = khh.substring(4).toInt
        // 更新Redis
        jedis.setbit(s"${TdrwtOperator.TDRWT_KHH_PREFIX}${DateUtil.getFormatNowDate()}_${yyb}_$channel", tempKhh, true)
      } else {
        logger.error(
          s"""
             |===================
             |TdrwtUpdate Operator khh is invaild....
             |khh: $khh
             |===================
            """.stripMargin)
      }
      val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
      val wtje = wtsl * wtjg * exchange
      val oldValue = tdrwt.getOrElse((yyb, channel), (0, BigDecimal(0)))
     // tdrwt.put((yyb, channel), (oldValue._1 + 1,oldValue._2+wtbs_marg ,oldValue._3 + wtje))
      tdrwt.put((yyb, channel), (oldValue._1 + 1, oldValue._2 + wtje))
    })
  }


  private def computeTywqqInsertRecord(
                                        tywqqInsertRecords: mutable.Map[String, TywqqRecord],
                                        tywqqrs: mutable.Map[String, Int]
                                      ) = {

    tywqqInsertRecords.foreach(record =>{

      val tywqqDetail: TywqqRecord = record._2
      val op = tywqqDetail.op
      val yyb = tywqqDetail.yyb
      val rowid=tywqqDetail.rowid
      val id=tywqqDetail.id
      val clzt=tywqqDetail.clzt
      if("8".equalsIgnoreCase(tywqqDetail.clzt)){
        logger.warn("ywqq31："+"op："+op+"rowid："+rowid+" ID "+id+"yyb："+yyb+" clzt "+clzt)
        val oldValue: Int = tywqqrs.getOrElse(yyb,0)
        tywqqrs.put(yyb,oldValue+1)
      }
    })
  }

  private def computeTkhxxInsertRecord(
                                        tkhxxInsertRecords: mutable.Map[String, TkhxxRecord],
                                        txhzh: mutable.Map[String, Int]
                                      ) = {

    tkhxxInsertRecords.foreach(record =>{

      val tkhxxDetail: TkhxxRecord = record._2
      val op = tkhxxDetail.op
      val khh = tkhxxDetail.khh
      val khzt=tkhxxDetail.khzt
      val xhrq=tkhxxDetail.xhrq
      logger.warn("xhzh1"+khzt+"xhrq"+xhrq+"***************")
        if(("3").equalsIgnoreCase(khzt) && xhrq.equalsIgnoreCase(DateUtil.getFormatNowDate().toString)){
          logger.warn("xhzh2"+khzt+"xhrq"+xhrq+"***************")
          val oldValue1 = txhzh.getOrElse(khzt, 0)
          txhzh.put(khzt, oldValue1 + 1)
        }
    })
  }
  private def computeTywsqlsInsertRecord(
                                        tywsqlsInsertRecords: mutable.Map[String, TFPYwsqlsRecord],
                                        tywsqlscount:mutable.Map[String,(Int,BigDecimal)]
                                      ) = {

    tywsqlsInsertRecords.foreach(record =>{

      val tywsqlsDetail: TFPYwsqlsRecord = record._2
      val op = tywsqlsDetail.op
      val ywdm = tywsqlsDetail.ywdm
      val rowid=tywsqlsDetail.rowid
      val wth=tywsqlsDetail.wth
      if("2".equalsIgnoreCase(tywsqlsDetail.clzt)&& "1".equalsIgnoreCase(tywsqlsDetail.ywdm)&&"1".equalsIgnoreCase(tywsqlsDetail.cljg)){
        val oldvalue = tywsqlscount.getOrElse("count",(0,BigDecimal(0)))
        logger.warn("otc4*******"+"op "+op+oldvalue._1+"   "+oldvalue._2 +"rowid "+rowid +"ywdm "+ywdm+" wth "+wth)
        tywsqlscount.put("count",(oldvalue._1+1,BigDecimal(0)))
      }
    })
  }
  private def computeTkhxxUpdateRecord(tkhxxUpdateRecords: mutable.Map[String, TkhxxRecord],
                                       tkhxxInsertRecords: mutable.Map[String, TkhxxRecord],
                                       //   tywqqMixWths: collection.Set[String],
                                       hbaseTkhxxReords: mutable.Map[String, mutable.Map[String, String]],
                                       txhzh: mutable.Map[String,Int]
                                      ) = {
    var slowMessageList: List[SlowMessageRecord] = List()
    tkhxxUpdateRecords.foreach(entry => {
      val khh = entry._1
      val tkhxxRecord = entry._2
      //val khh =tkhxxRecord.khh
      var khzt = tkhxxRecord.khzt
      var xhrq: String = tkhxxRecord.xhrq
      //Thread.sleep(3000)
      entry
      logger.warn("xhzh1" + khzt + "xhrq" + xhrq + " khh " + khh + "***************")

      if (("3").equalsIgnoreCase(khzt) && xhrq.equalsIgnoreCase(DateUtil.getFormatNowDate().toString)) {
        logger.warn("xhzh2" + khzt + "xhrq" + xhrq + " khh " + khh + "***************")
        val oldValue1 = txhzh.getOrElse(khzt, 0)
        txhzh.put(khzt, oldValue1 + 1)
      }
   })
  }

  private def computeTywqqUpdateRecord(tywqqUpdateRecords: mutable.Map[String, TywqqRecord],
                                       tywqqInsertRecords: mutable.Map[String, TywqqRecord],
                                    //   tywqqMixWths: collection.Set[String],
                                       hbaseTywqqReords: mutable.Map[String, mutable.Map[String, String]],
                                       tywqqrs: mutable.Map[String,Int]
                                      ) = {
    var slowMessageList: List[SlowMessageRecord] = List()
    tywqqUpdateRecords.foreach(entry => {
      val rowid = entry._1
      val tywqqRecord = entry._2
      val rowid1 =tywqqRecord.rowid
      val id=tywqqRecord.id
      var khh: String = null
      var yyb: String = "nvg"
      var sqrq: Int= 0
      var clzt: String = tywqqRecord.clzt
      //Thread.sleep(3000)


    //  logger.warn("ywqqupdate1："+"op："+tywqqRecord.op+"rowid："+rowid1+" ID "+id+"clzt "+clzt+"yyb："+yyb+"khh "+khh+"sqrq "+sqrq)
      if("8".equalsIgnoreCase(clzt)
        && tywqqRecord.time == DateUtil.getFormatNowDate().toString
      ) {
        if (tywqqInsertRecords.contains(id)) {
          val tywqqDetail = tywqqInsertRecords.get(id).get
          khh = tywqqDetail.khh
          yyb = tywqqDetail.yyb
          sqrq = tywqqDetail.sqrq
//          logger.warn("ywqqupdate21：" + "op：" + tywqqRecord.op + "rowid：" + rowid1 +" ID "+id+ "clzt " + clzt + "yyb：" + yyb + "khh " + khh + "sqrq " + sqrq)
//          val oldValue: Int = tywqqrs.getOrElse(yyb, 0)
//          tywqqrs.put(yyb, oldValue + 1)
        } else {
          // HBase中获取
         // val  hbaseTywqqReords = getAllTywqqFromHBaseOrPartition(tywqqInsertRecords, tywqqUpdateRecords)
            val data1 = hbaseTywqqReords.get(id)
            if(!data1.isEmpty){
                val data = data1.get
                //这个kafka的消息只有一个clzt=8
                khh = data.get("KHH").getOrElse("")
                yyb = data.get("YYB").getOrElse("")
                sqrq = data.get("SQRQ").getOrElse("0").toInt
//                logger.warn("ywqqupdate22：" + "op：" + tywqqRecord.op + "rowid：" + rowid1 +" ID "+id+ "clzt " + clzt + "yyb：" + yyb + "khh " + khh + "sqrq " + sqrq)
//                val oldValue: Int = tywqqrs.getOrElse(yyb, 0)
//                tywqqrs.put(yyb, oldValue + 1)
            }

        }

        logger.warn("ywqqupdate23"+"yyb" + yyb+ "clzt" + clzt + "op：" + tywqqRecord.op+" ID "+id  + "khh " + khh + "sqrq " + sqrq)
        val max=20
        val version = tywqqRecord.version
        if(sqrq==DateUtil.getFormatNowDate()){
          if("nvg".equalsIgnoreCase(yyb)){
            if(version<max){
              logger.warn("ywqqversion "+version+" content "+tywqqRecord)
              tywqqRecord.version = version + 1
              val slowMessage = SlowMessageRecord("SECURITIES_TYWQQ", tywqqRecord)
              slowMessageList ::= slowMessage
            }
            else{
              logger.warn("ywqqversion2"+"yyb" + yyb+ "clzt" + clzt+" ID "+id +version)
              //            val oldValue: Int = tywqqrs.getOrElse(yyb, 0)
              //            tywqqrs.put(yyb, oldValue + 1)
            }
          }else if(!"nvg".equalsIgnoreCase(yyb)){
            val oldValue: Int = tywqqrs.getOrElse(yyb, 0)
            tywqqrs.put(yyb, oldValue + 1)
          }
        }
      }
    })
    slowMessageList
  }

//  computeTywsqlsUpdateRecord(tFPYwsqlsUpdateRecords, tFPYwsqlsInsertRecords,tywsqlsMixWths,hbaseTywsqlsReords,tfpywsqlscount)

  private def computeTywsqlsUpdateRecord(tywsqlsUpdateRecords: mutable.Map[String, TFPYwsqlsRecord],
                                       tywsqlsInsertRecords: mutable.Map[String, TFPYwsqlsRecord],
                                     //  tywsqlsMixWths: collection.Set[String],
                                       hbaseTywsqlsReords: mutable.Map[String, mutable.Map[String, String]],
                                       tywsqlscount:mutable.Map[String,(Int,BigDecimal)],
                                         tfpywsqls:mutable.Map[String,(Int,BigDecimal)]
                                      ) = {
    var slowMessageList: List[SlowMessageRecord] = List()
    tywsqlsUpdateRecords.foreach(entry => {
     // val rowid = entry._1
      val tywsqlsRecord = entry._2
    //  val rowid1 =tywsqlsRecord.rowid
      val wth =tywsqlsRecord.wth
      var khh: String = tywsqlsRecord.khh
      var op:String =tywsqlsRecord.op
      //大部分数据都没有ywdm
      var ywdm: String = tywsqlsRecord.ywdm
      var cljg: String = tywsqlsRecord.cljg
      var djrq: String = tywsqlsRecord.djrq
      var clzt: String = tywsqlsRecord.clzt
      var cpfl: String = tywsqlsRecord.cpfl
      var ywje: BigDecimal = tywsqlsRecord.ywje





      logger.warn("ywsqlsupdate1 "+ " "+"wth "+wth+"clzt "+clzt+"ywdm "+ywdm+"cljg "+cljg+"cpfl "+cpfl)
      if("2".equalsIgnoreCase(clzt)
       //&& tywsqlsRecord.time == DateUtil.getFormatNowDate().toString
      ){
       if(tywsqlsInsertRecords.contains(wth)){
          val tywsqlsDetail = tywsqlsInsertRecords.get(wth).get
          if(!"1".equalsIgnoreCase(ywdm)){
            if("nvl".equalsIgnoreCase(ywdm)){
              ywdm=tywsqlsDetail.ywdm
              if("nvl".equalsIgnoreCase(cljg)){
                cljg=tywsqlsDetail.cljg
                if("nvl".equalsIgnoreCase(cpfl)){
                  cpfl=tywsqlsDetail.cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }else{
                  cpfl=cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }
              }else{
                cljg=cljg
                if("nvl".equalsIgnoreCase(cpfl)){
                  cpfl=tywsqlsDetail.cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }else{
                  cpfl=cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }
              }
            }else{
              ywdm=ywdm
              if("nvl".equalsIgnoreCase(cljg)){
                cljg=tywsqlsDetail.cljg
                if("nvl".equalsIgnoreCase(cpfl)){
                  cpfl=tywsqlsDetail.cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }else{
                  cpfl=cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }
              }else{
                cljg=cljg
                if("nvl".equalsIgnoreCase(cpfl)){
                  cpfl=tywsqlsDetail.cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }else{
                  cpfl=cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }
              }
            }
          }else{
//            if("nvl".equalsIgnoreCase(ywdm)){
//              ywdm=tywsqlsDetail.ywdm
//              if("nvl".equalsIgnoreCase(cljg)){
//                cljg=tywsqlsDetail.cljg
//                if("nvl".equalsIgnoreCase(cpfl)){
//                  cpfl=tywsqlsDetail.cpfl
//                  if("0".equalsIgnoreCase(ywje.toString())){
//                    ywje=tywsqlsDetail.ywje
//                  }
//                  ywje=ywje
//                }
//                cpfl=cpfl
//                ywje=ywje
//              }
//              cljg=cljg
//              cpfl=cpfl
//              ywje=ywje
//            }else{
              ywdm=ywdm
              if("nvl".equalsIgnoreCase(cljg)){
                cljg=tywsqlsDetail.cljg
                if("nvl".equalsIgnoreCase(cpfl)){
                  cpfl=tywsqlsDetail.cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }else{
                  cpfl=cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }
              }else{
                cljg=cljg
                if("nvl".equalsIgnoreCase(cpfl)){
                  cpfl=tywsqlsDetail.cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }else{
                  cpfl=cpfl
                  if("0".equalsIgnoreCase(ywje.toString())){
                    ywje=tywsqlsDetail.ywje
                  }
                  ywje=ywje
                }
              }
   //         }
          }
        }else{
          // HBase中获取
          val data1 = hbaseTywsqlsReords.get(wth)
          if(!data1.isEmpty){
            val data = data1.get
            if(!"1".equalsIgnoreCase(ywdm)){
              if("nvl".equalsIgnoreCase(ywdm)){
                ywdm = data.get("YWDM").getOrElse("nvl")
                if("nvl".equalsIgnoreCase(cljg)){
                  cljg = data.get("CLJG").getOrElse("nvl")
                  if("nvl".equalsIgnoreCase(cpfl)){
                    cpfl=data.get("CPFL").getOrElse("nvl")
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }else{
                    cpfl=cpfl
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }
                }else{
                  cljg=cljg
                  if("nvl".equalsIgnoreCase(cpfl)){
                    cpfl=data.get("CPFL").getOrElse("nvl")
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }else{
                    cpfl=cpfl
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }
                }
              }else{
                ywdm=ywdm
                if("nvl".equalsIgnoreCase(cljg)){
                  cljg = data.get("CLJG").getOrElse("nvl")
                  if("nvl".equalsIgnoreCase(cpfl)){
                    cpfl=data.get("CPFL").getOrElse("nvl")
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }else{
                    cpfl=cpfl
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }
                }else{
                  cljg=cljg
                  if("nvl".equalsIgnoreCase(cpfl)){
                    cpfl=data.get("CPFL").getOrElse("nvl")
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }else{
                    cpfl=cpfl
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }
                }
              }
            }else{
//              if("nvl".equalsIgnoreCase(ywdm)){
//                ywdm = data.get("YWDM").getOrElse("nvl")
//                if("nvl".equalsIgnoreCase(cljg)){
//                  cljg = data.get("CLJG").getOrElse("nvl")
//                  if("nvl".equalsIgnoreCase(cpfl)){
//                    cpfl=data.get("CPFL").getOrElse("nvl")
//                    if("0".equalsIgnoreCase(ywje.toString())){
//                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
//                    }
//                    ywje=ywje
//                  }else{
//                    cpfl=cpfl
//                    if("0".equalsIgnoreCase(ywje.toString())){
//                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
//                    }
//                    ywje=ywje
//                  }
//                }else{
//                  cljg=cljg
//                  if("nvl".equalsIgnoreCase(cpfl)){
//                    cpfl=data.get("CPFL").getOrElse("nvl")
//                    if("0".equalsIgnoreCase(ywje.toString())){
//                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
//                    }
//                    ywje=ywje
//                  }else{
//                    cpfl=cpfl
//                    if("0".equalsIgnoreCase(ywje.toString())){
//                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
//                    }
//                    ywje=ywje
//                  }
//                }
//              }else

                ywdm=ywdm
                if("nvl".equalsIgnoreCase(cljg)){
                  cljg = data.get("CLJG").getOrElse("nvl")
                  if("nvl".equalsIgnoreCase(cpfl)){
                    cpfl=data.get("CPFL").getOrElse("nvl")
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }else{
                    cpfl=cpfl
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }
                }else{
                  cljg=cljg
                  if("nvl".equalsIgnoreCase(cpfl)){
                    cpfl=data.get("CPFL").getOrElse("nvl")
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }else{
                    cpfl=cpfl
                    if("0".equalsIgnoreCase(ywje.toString())){
                      ywje = BigDecimal(data.getOrElse("YWJE","0"))
                    }
                    ywje=ywje
                  }
                }
            }
//            if("1".equalsIgnoreCase(cljg)){
//              ywdm = data.get("YWDM").getOrElse("nvl")
//              cpfl=data.get("CPFL").getOrElse("nvl")
//              ywje = BigDecimal(data.getOrElse("YWJE","0"))
//            }else{
//              ywdm = data.get("YWDM").getOrElse("nvl")
//              cljg = data.get("CLJG").getOrElse("nvl")
//              cpfl=data.get("CPFL").getOrElse("nvl")
//              ywje = BigDecimal(data.getOrElse("YWJE","0"))
//            }
          }
        }
        val max=20
        val version = tywsqlsRecord.version
        logger.warn("YWSQLSUPDATE3" + " "+"wth "+wth+"clzt "+clzt+"ywdm "+ywdm+"cljg "+cljg+"djrq "+djrq+"cpfl "+cpfl)
        if("1".equalsIgnoreCase(ywdm)&& "1".equalsIgnoreCase(cljg)) {
          val oldvalue = tywsqlscount.getOrElse("count", (0, BigDecimal(0)))
            logger.warn("ywsqlsupdate4*******" + "op " + op + "wth " + wth)
          tywsqlscount.put("count", (oldvalue._1 + 1, BigDecimal(0)))
        }else if(!"1".equalsIgnoreCase(ywdm)&& "1".equalsIgnoreCase(cljg)&&"14".equalsIgnoreCase(cpfl)) {
          logger.warn("ywsqlsupdate2.4 " + "clzt " + clzt + "ywdm " + ywdm + "cljg " + cljg + "djrq " + djrq+"cpfl "+cpfl)
          val oldvalue = tfpywsqls.getOrElse("ins",(0,BigDecimal(0)))
          tfpywsqls.put("ins",(oldvalue._1+1,oldvalue._2+ywje))
        }else{
          if(version<max) {
            logger.warn("ywsqlsversion "+version+" content "+tywsqlsRecord)
            tywsqlsRecord.version = version + 1
            val slowMessage = SlowMessageRecord("SECURITIES_TYWSQLSKH", tywsqlsRecord)
            slowMessageList ::= slowMessage
          }else{
            logger.warn("ywsqlsversion2 "+version+" content "+tywsqlsRecord)
          }
        }



//
//       // logger.warn("ywsqlsupdate1.2 "+tywsqlsRecord)
//        if("1".equalsIgnoreCase(ywdm)&& "1".equalsIgnoreCase(cljg)){
//              ywdm=ywdm
//              cljg=cljg
//          logger.warn("ywsqlsupdate2.1 "+"clzt "+clzt+"ywdm "+ywdm+"cljg "+cljg+"djrq "+djrq)
//          if("1".equalsIgnoreCase(ywdm)&& "1".equalsIgnoreCase(cljg)) {
//            val oldvalue = tywsqlscount.getOrElse("count", (0, BigDecimal(0)))
//            logger.warn("otc4*******" + "op " + op + "rowid " + rowid)
//            tywsqlscount.put("count", (oldvalue._1 + 1, BigDecimal(0)))
//          }
//        }else if(tywsqlsInsertRecords.contains(wth)){
//          val tywsqlsDetail = tywsqlsInsertRecords.get(wth).get
//          //此update 的数据 有可能都有
//          if("1".equalsIgnoreCase(cljg)){
//            ywdm=tywsqlsDetail.ywdm
//          }else{
//            ywdm=tywsqlsDetail.ywdm
//            cljg=tywsqlsDetail.cljg
//          }
//            logger.warn("ywsqlsupdate2.2 "+"clzt "+clzt+ "wth " + wth+"ywdm "+ywdm+"cljg "+cljg+"djrq "+djrq)
//            if("1".equalsIgnoreCase(ywdm)&& "1".equalsIgnoreCase(cljg)) {
//              val oldvalue = tywsqlscount.getOrElse("count", (0, BigDecimal(0)))
//              logger.warn("otc4*******" + "op " + op + "rowid " + rowid)
//              tywsqlscount.put("count", (oldvalue._1 + 1, BigDecimal(0)))
//            }
//        }else{
//          // HBase中获取
//          val data1 = hbaseTywsqlsReords.get(wth)
//              logger.warn("hbaseywsqls "+data1)
//          if(!data1.isEmpty){
//
//            val data = data1.get
//            if("1".equalsIgnoreCase(cljg)){
//              ywdm = data.get("YWDM").getOrElse("nvl")
//            }else{
//              ywdm = data.get("YWDM").getOrElse("nvl")
//              cljg = data.get("CLJG").getOrElse("nvl")
//            }
//            logger.warn("ywsqlsupdate2.3" + " "+"clzt "+clzt+"ywdm "+ywdm+"cljg "+cljg+"djrq "+djrq)
//            if("1".equalsIgnoreCase(ywdm)&& "1".equalsIgnoreCase(cljg)) {
//              val oldvalue = tywsqlscount.getOrElse("count", (0, BigDecimal(0)))
//              logger.warn("otc4*******" + "op " + op + "rowid " + rowid+ "wth " + wth)
//              tywsqlscount.put("count", (oldvalue._1 + 1, BigDecimal(0)))
//            }
//          }
//        }
//        logger.warn("ywsqlsupdate2 "+"clzt "+clzt+"ywdm "+ywdm+"cljg "+cljg+"djrq "+djrq)
//        if("1".equalsIgnoreCase(ywdm)&& "1".equalsIgnoreCase(cljg)){
//          val oldvalue = tywsqlscount.getOrElse("count",(0,BigDecimal(0)))
//          logger.warn("otc4*******"+"op "+op+"rowid "+rowid)
//          tywsqlscount.put("count",(oldvalue._1+1,BigDecimal(0)))
//        }

      }

    })
    slowMessageList
  }

  private def  computeTywqq( tywqq: mutable.Map[Int,TywqqRecord],
                             tywqqrs: mutable.Map[String,Int]): Unit ={
    tywqq.foreach(record =>{

      val tywqqDetail: TywqqRecord = record._2
         val op = tywqqDetail.op
         val yyb = tywqqDetail.yyb
         val rowid=tywqqDetail.rowid
      logger.warn("ywqq3："+"op："+op+"rowid："+rowid+"yyb："+yyb)
      val oldValue: Int = tywqqrs.getOrElse(yyb,0)
      tywqqrs.put(yyb,oldValue+1)
    }
    )
  }

  /**
    * 从当前批次的Insert Tdrwt记录或者从HBase中关联出所有能关联到的Tdrwt记录，便于后续 Tdrwt Update记录及Tssc记录的处理
    *
    * @param tdrwtInsertRecords
    * @param tdrwtUpdateRecords
    * @param tsscjRecords
    * @return
    */
  private def getAllTdrwtFromHBaseOrPartition(tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                                              tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord],
                                              tsscjRecords: mutable.Map[String, TsscjRecord]) = {
    val tdrwtUpdateWths = tdrwtUpdateRecords.keySet
    val tdrwtInsertWths = tdrwtInsertRecords.keySet
    // tsscjRecords记录的key 是 wth_cjbh
    val tsscjWths = tsscjRecords.keySet.map(key => key.split("_")(0))
    // tdrwt insert 与 update wth交集  已有的数据
    val tdrwtMixWths = tdrwtUpdateWths & tdrwtInsertWths
    // tdrwt  update wth独享集  新增的数据
    val tdrwtOnlyUpdateWths = tdrwtUpdateWths -- tdrwtMixWths
    // tsscj insert 与 update wth交集
    val tsscjMixWths = tsscjWths & tdrwtInsertWths
    // tsscj  update wth独享集
    val tsscjOnlyWths = tsscjWths -- tsscjMixWths
    val onlyWths = tsscjOnlyWths ++ tdrwtOnlyUpdateWths
    // 批量获取Hbase中的  两个独享集里面的所有数据
    val hbaseTdrwtReords: mutable.Map[String, mutable.Map[String, String]] = HBaseUtil.getRecordsFromHBaseByKeys(
      ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE),
      ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS),
      onlyWths)
    (tdrwtMixWths, tsscjMixWths, hbaseTdrwtReords)
  }
  private def getAllTkhxxFromHBaseOrPartition(tkhxxInsertRecords: mutable.Map[String, TkhxxRecord],
                                               tkhxxUpdateRecords: mutable.Map[String, TkhxxRecord]
                                              ) = {
    val tkhxxUpdateWths = tkhxxInsertRecords.keySet
    val tkhxxInsertWths = tkhxxUpdateRecords.keySet
    // 批量获取Hbase中的  两个独享集里面的所有数据
    val hbaseTkhxxReords: mutable.Map[String, mutable.Map[String, String]] = HBaseUtil.getTkhxxRecordsFromHBaseByKeys(
      "trade_monitor:tkhxx_khh",
      "cf",
      tkhxxUpdateWths)
    hbaseTkhxxReords
  }
  private def getAllTywqq1FromHBaseOrPartition(tywqqInsertRecords: mutable.Map[String, TywqqRecord],
                                               tywqqUpdateRecords: mutable.Map[String, TywqqRecord]
                                              ) = {
    val tywqqUpdateWths = tywqqUpdateRecords.keySet
    val tywqqInsertWths = tywqqInsertRecords.keySet
//    // tsscjRecords记录的key 是 wth_cjbh
//    val tsscjWths = tsscjRecords.keySet.map(key => key.split("_")(0))
//    // tdrwt insert 与 update wth交集  已有的数据
//    val tdrwtMixWths = tdrwtUpdateWths & tdrwtInsertWths
//    // tdrwt  update wth独享集  新增的数据
//    val tdrwtOnlyUpdateWths = tdrwtUpdateWths -- tdrwtMixWths
//    // tsscj insert 与 update wth交集
//    val tsscjMixWths = tsscjWths & tdrwtInsertWths
//    // tsscj  update wth独享集
//    val tsscjOnlyWths = tsscjWths -- tsscjMixWths
//    val onlyWths = tsscjOnlyWths ++ tdrwtOnlyUpdateWths
    // 批量获取Hbase中的  两个独享集里面的所有数据
    val hbaseTywqqReords: mutable.Map[String, mutable.Map[String, String]] = HBaseUtil.getTywqqRecordsFromHBaseByKeys(
    "trade_monitor:tywqq2_rowid",
    "cf",
  tywqqUpdateWths)
    hbaseTywqqReords
  }

  private def getAllTywsqls1FromHBaseOrPartition(tywsqlsInsertRecords: mutable.Map[String, TFPYwsqlsRecord],
                                                 tywsqlsUpdateRecords: mutable.Map[String, TFPYwsqlsRecord]
                                              ) = {
    val tywsqlsUpdateWths = tywsqlsUpdateRecords.keySet
    val tywsqlsInsertWths = tywsqlsInsertRecords.keySet
    val hbaseTywsqlsReords: mutable.Map[String, mutable.Map[String, String]] = HBaseUtil.getRecordsFromHBaseByKeys(
      "trade_monitor:tywsqls_rowid",
    "cf",
      tywsqlsUpdateWths)
    hbaseTywsqlsReords
  }

  private def getAllTywqqFromHBaseOrPartition(tywqqInsertRecords: mutable.Map[String, TywqqRecord],
                                              tywqqUpdateRecords: mutable.Map[String, TywqqRecord]
                                             ) = {
    val tywqqUpdateWths = tywqqUpdateRecords.keySet
    val tywqqInsertWths = tywqqInsertRecords.keySet
  //  val tywqqMixWths = tywqqUpdateWths & tywqqInsertWths

    // 批量获取Hbase中的  两个独享集里面的所有数据
   // logger.warn("ywqqfetch "+ConfigurationManager.getProperty(Constants.HBASE_TYWQQ_ROWID_TABLE)+" "+ConfigurationManager.getProperty(Constants.HBASE_ROWID_INFO_FAMILY_COLUMNS))
    val hbaseTywqqReords: mutable.Map[String, mutable.Map[String, String]] = HBaseUtil.getRecordsFromywqqALLHBaseByKeys(
      "trade_monitor:tywqq2_rowid",
      "cf")
     hbaseTywqqReords
  }
  private def getAllTywsqlsFromHBaseOrPartition(tywsqlsInsertRecords: mutable.Map[String, TFPYwsqlsRecord],
                                              tywsqlsUpdateRecords: mutable.Map[String, TFPYwsqlsRecord]
                                             ) = {
    val tywsqlsUpdateWths = tywsqlsUpdateRecords.keySet
    val tywsqlsInsertWths = tywsqlsInsertRecords.keySet
    val tywsqlsMixWths = tywsqlsUpdateWths & tywsqlsInsertWths

    // 批量获取Hbase中的  两个独享集里面的所有数据
    //logger.warn("ywsqlsfetch "+ConfigurationManager.getProperty(Constants.HBASE_Tywsqls_ROWID_TABLE)+" "+ConfigurationManager.getProperty(Constants.HBASE_ROWID_INFOF_FAMILY_COLUMNS))
    val hbaseTywsqlsReords: mutable.Map[String, mutable.Map[String, String]] = HBaseUtil.getRecordsFromywsqlsALLHBaseByKeys(
      "trade_monitor:tywsqls_rowid",
      "cf")
    (tywsqlsMixWths, hbaseTywsqlsReords)
  }

  /**
    * 分类不同来源的记录，并针对不同的消息记录进行不同的处理
    *
    * @param records
    * @param tdrwtInsertRecords
    * @param tdrwtUpdateRecords
    * @param tsscjRecords
    * @param tkhxx
    * @param tdrzjmx
    * @param tFPInsertYwsqls
    * @param exchangeMapBC
    *
    */
  private def classifyAndComputeBaseRecord(records: Iterator[(String, Iterable[BaseRecord])],
                                           tdrwtInsertRecords: mutable.Map[String, TdrwtRecord],
                                           tdrwtUpdateRecords: mutable.Map[String, TdrwtRecord],
                                           tsscjRecords: mutable.Map[String, TsscjRecord],
                                           tkhxx: mutable.Map[String, Int],
                                           tkhxxInsertRecords:mutable.Map[String,TkhxxRecord],
                                           tkhxxUpdateRecords:mutable.Map[String,TkhxxRecord],
                                           txhzh: mutable.Map[String, Int],
                                           tdrzjmx: mutable.Map[String, BigDecimal],
                                           tFPYwsqlsRecords:mutable.Map[String,TFPYwsqlsRecord],
                                           tFPInsertYwsqls:mutable.Map[String,TFPYwsqlsRecord],
                                           tFPUpdateRecords:mutable.Map[String,TFPYwsqlsRecord],
                                           tgdzh: mutable.Map[String, Int],
                                           tjjzh: mutable.Map[String, Int],
                                           tzjzh: mutable.Map[String, Int],
                                           tywqq: mutable.Map[Int, TywqqRecord],
                                           tywqqInsertRecords: mutable.Map[String, TywqqRecord],
                                           tywqqUpdateRecords: mutable.Map[String, TywqqRecord],
                                           exchangeMapBC: Broadcast[Map[String, BigDecimal]]) = {
    records.foreach(recorditer => {
      recorditer._2.foreach(record => {
        record match {
          // 新开户
          case tkhxxRecord: TkhxxRecord => {
            tkhxxRecord.op match {
              case "INSERT" =>{
                if((!TkhxxOperator.QT.equalsIgnoreCase(tkhxxRecord.jgbz))&&tkhxxRecord.khrq == DateUtil.getFormatNowDate()){
                  val oldValue = tkhxx.getOrElse(tkhxxRecord.jgbz, 0)
                  tkhxx.put(tkhxxRecord.jgbz, oldValue + 1)
                }
               // tkhxxInsertRecords.put(tkhxxRecord.khh,tkhxxRecord)
                val khzt=tkhxxRecord.khzt
                val xhrq=tkhxxRecord.xhrq
                val khh=tkhxxRecord.khh
                logger.warn("xhzh1" + khzt + "xhrq" + xhrq + " khh " + khh + "***************")
                if (("3").equalsIgnoreCase(khzt)
                  && xhrq.equalsIgnoreCase(DateUtil.getFormatNowDate().toString)
                ) {
                 logger.warn("xhzh2" + khzt + "xhrq" + xhrq + " khh " + khh + "***************")
                  val oldValue1 = txhzh.getOrElse(khzt, 0)
                  txhzh.put(khzt, oldValue1 + 1)
                }
              }
              case "UPDATE" =>{
                //tkhxxUpdateRecords.put(tkhxxRecord.khh,tkhxxRecord)
                val khzt=tkhxxRecord.khzt
                val xhrq=tkhxxRecord.xhrq
                val khh=tkhxxRecord.khh
                logger.warn("xhzh3" + khzt + "xhrq" + xhrq + " khh " + khh + "***************")
                if (("3").equalsIgnoreCase(khzt)
                 && xhrq.equalsIgnoreCase(DateUtil.getFormatNowDate().toString)
                ) {
                 logger.warn("xhzh4" + khzt + "xhrq" + xhrq + " khh " + khh + "***************")
                  val oldValue1 = txhzh.getOrElse(khzt, 0)
                  txhzh.put(khzt, oldValue1 + 1)
                }
              }
              case _=> println("qita")
            }
            // logger.warn("**************jisuan************"+tkhxx.getOrElse(tkhxxRecord.jgbz,0))
          }
          // 实时成交 (wth_cjbh,record)
          case tsscjRecord: TsscjRecord => {
            tsscjRecords.put(s"${tsscjRecord.wth}_${tsscjRecord.cjbh}", tsscjRecord)
          }
          // 资金转入转出
          case tdrzjmxRecord: TdrzjmxRecord => {
            val bz = tdrzjmxRecord.bz.toUpperCase
            val exchange = exchangeMapBC.value.getOrElse(bz, BigDecimal(1))
            val opje = tdrzjmxRecord.je * exchange
            val oldValue = tdrzjmx.getOrElse(tdrzjmxRecord.op, BigDecimal(0))
            tdrzjmx.put(tdrzjmxRecord.op, oldValue + opje)
          }
          // 实时委托
          case tdrwtRecord: TdrwtRecord => {
            tdrwtRecord.op match {
              case "INSERT" => tdrwtInsertRecords.put(tdrwtRecord.wth, tdrwtRecord)
              case "UPDATE" => tdrwtUpdateRecords.put(tdrwtRecord.wth, tdrwtRecord)
              case _ =>
            }
          }
          //            OTC实时委托 20201022
          case tfpywsqlsRecord: TFPYwsqlsRecord=>{
            if (
              tfpywsqlsRecord.djrq.equalsIgnoreCase(DateUtil.getFormatNowDate().toString)
            ){
              logger.warn("ywsqls1 "+tFPYwsqlsRecords)
              tfpywsqlsRecord.op match {
                case "INSERT" =>tFPInsertYwsqls.put(tfpywsqlsRecord.wth,tfpywsqlsRecord)
                case "UPDATE" =>tFPUpdateRecords.put(tfpywsqlsRecord.wth,tfpywsqlsRecord)
                case _=> println("qita")
              }
            }
          }
          // 股东账户开户数
          case tgdzhRecord: TgdzhRecord => {
            val oldValue = tgdzh.getOrElse(tgdzhRecord.gdzt, 0)
            tgdzh.put(tgdzhRecord.gdzt, oldValue+1)
          }
          case tzjzhRecord: TzjzhRecord => {
            val oldValue = tzjzh.getOrElse("bankzh", 0)
            tzjzh.put("bankzh", oldValue+1)
          }
          case tjjzhRecord: TjjzhRecord => {
            logger.warn("jjzh3 "+tjjzhRecord)
              val oldValue1 =tjjzh.getOrElse(tjjzhRecord.zhzt,0)
              tjjzh.put(tjjzhRecord.zhzt,oldValue1+1)
          }
          case tywqqRecord: TywqqRecord => {
            tywqqRecord.op match {
              case "INSERT" => {
                if (tywqqRecord.sqrq==DateUtil.getFormatNowDate()){
                  tywqqInsertRecords.put(tywqqRecord.id, tywqqRecord)
                }
              }
              case "UPDATE" => tywqqUpdateRecords.put(tywqqRecord.id, tywqqRecord)
              case _ =>
            }
          }

          case _ =>
        }
      })
    })
  }

  /**
    * 批量插入HBase
    *
    * @param tdrwtInsertRecords
    */
  private def putTdrwtRecordsToHBase(tdrwtInsertRecords: mutable.Map[String, TdrwtRecord]): Unit = {

   // val values: Iterable[TdrwtRecord] = tdrwtInsertRecords.values

    val puts = tdrwtInsertRecords.values
      .map(tdrwtRecord =>
        HBaseUtil.parseTdrwtToPut(tdrwtRecord, ConfigurationManager.getProperty(Constants.HBASE_WTH_INFO_FAMILY_COLUMNS))
      )

      .toList
    HBaseUtil.BatchMultiColMessageToHBase(ConfigurationManager.getProperty(Constants.HBASE_TDRWT_WTH_TABLE), puts)
  }
  private def putTkhxxRecordsToHBase(tkhxxInsertRecords:mutable.Map[String,TkhxxRecord]): Unit = {

    // val values: Iterable[TdrwtRecord] = tdrwtInsertRecords.values
    //logger.warn("ywqqinsert "+tywqqInsertRecords)
    if(!tkhxxInsertRecords.isEmpty){
      //  logger.warn("ywqqputs1 "+ConfigurationManager.getProperty(Constants.HBASE_ROWID_INFO_FAMILY_COLUMNS)+" "+Constants.HBASE_ROWID_INFO_FAMILY_COLUMNS)
      val puts = tkhxxInsertRecords.values
        .map(tkhxxRecord =>
          //          hbase.tywqq.rowid.table=trade_monitor:tywqq_rowid
          //            hbase.rowid.info.family.columns=infoo
          HBaseUtil.parseTkhxxToPut(tkhxxRecord, "cf")
        )
        .toList
      //    logger.warn("ywqqputs2 "+puts.isEmpty)
      HBaseUtil.BatchMultiColMessageToHBase("trade_monitor:tkhxx_khh", puts)
    }
  }
  private def putTywqqRecordsToHBase(tywqqInsertRecords: mutable.Map[String, TywqqRecord]): Unit = {

   // val values: Iterable[TdrwtRecord] = tdrwtInsertRecords.values
    //logger.warn("ywqqinsert "+tywqqInsertRecords)
    if(!tywqqInsertRecords.isEmpty){
    //  logger.warn("ywqqputs1 "+ConfigurationManager.getProperty(Constants.HBASE_ROWID_INFO_FAMILY_COLUMNS)+" "+Constants.HBASE_ROWID_INFO_FAMILY_COLUMNS)
      val puts = tywqqInsertRecords.values
        .map(tywqqRecord =>
//          hbase.tywqq.rowid.table=trade_monitor:tywqq_rowid
//            hbase.rowid.info.family.columns=infoo
          HBaseUtil.parseTywqqToPut(tywqqRecord, "cf")
        )
        .toList
  //    logger.warn("ywqqputs2 "+puts.isEmpty)
      HBaseUtil.BatchMultiColMessageToHBase("trade_monitor:tywqq2_rowid", puts)
    }
  }

  private def putTywsqlsRecordsToHBase(tFPYwsqlsInsertRecords: mutable.Map[String, TFPYwsqlsRecord]): Unit = {

   // val values: Iterable[TdrwtRecord] = tdrwtInsertRecords.values
  //  logger.warn("ywsqlsinsert0 "+tFPYwsqlsInsertRecords)
    if(!tFPYwsqlsInsertRecords.isEmpty){
   //   logger.warn("ywsqlsputs11 "+ConfigurationManager.getProperty(Constants.HBASE_Tywsqls_ROWID_TABLE)+" "+Constants.HBASE_ROWID_INFOF_FAMILY_COLUMNS)
      val puts = tFPYwsqlsInsertRecords.values
        .map(tywsqlsRecord =>
          HBaseUtil.parseTywsqlsToPut(tywsqlsRecord, "cf")
        )
        .toList
  //    logger.warn("ywsqlsputs22 "+puts.isEmpty)
      HBaseUtil.BatchMultiColMessageToHBase("trade_monitor:tywsqls_rowid", puts)
    }
  }
  private def putTcjjeRecordsToHBase(wth:String,gdh:String,cjje:BigDecimal, tup : mutable.Map[String,(String ,String , BigDecimal)]): Unit = {

    val tuple: (String ,String , BigDecimal) = (wth,gdh,cjje)
   // val tup : mutable.Map[String,(String ,String , BigDecimal)]=Map()
    tup.put(Random.nextInt(1000).toString,tuple)
    val puts = tup.values
      .map(tuple3=>{
        HBaseUtil.parseTsscjToPut(tuple3,"cf")
      }).toList
    logger.warn("Tsscjputs22 "+puts.isEmpty)
    HBaseUtil.BatchMultiColMessageToHBase("trade_monitor:cjje_rowid", puts)


//    下面是 单个插入一条的
//        val put: Put = HBaseUtil.parseTcjjeToPut(tuple, "cf")
//    logger.warn("cjjeputs22 "+put.isEmpty)
//    HBaseUtil.insertSingleColMessageToHBase(
//      "trade_monitor:cjje_rowid",
//      tuple._1,
//      "cf",
//      "cjje",
//      tuple._2.toString()
//    )
  }
}
