package com.hbzq.bigdata.spark

import java.sql.Timestamp
import java.time.LocalDateTime

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import com.hbzq.bigdata.spark.operator.rdd._
import com.hbzq.bigdata.spark.operator.record.BaseRecordOperator
import com.hbzq.bigdata.spark.operator.runnable.{FlushRedisToMysqlTask, RedisDelKeyTask}
import com.hbzq.bigdata.spark.utils._
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import scalikejdbc.{DB, SQL}

import scala.collection.mutable


/**
  * describe:
  * create on 2020/05/26
  *
  * 实时交易监控程序入口
  *
  * @author hqbhoho
  * @version [v1.0]
  *
  */
object RealTimeTradeMonitor {
  private[this] val logger = Logger.getLogger(RealTimeTradeMonitor.getClass)

  def main(args: Array[String]): Unit = {
    // 获取Kafka配置
    var (topics, kafkaParams) = ConfigurationManager.getKafkaConfig()
    // 启动  先删除Redis  key
    RedisDelKeyTask().run()
    var ssc: StreamingContext = startApp(topics, kafkaParams)
    if (!ssc.awaitTerminationOrTimeout(DateUtil.getDurationTime())) {
      logger.warn(
        s"""
           |====================
           |Begin to stop APP......
           |${LocalDateTime.now()}
           |====================
        """.stripMargin)
      ssc.stop(true, true)
    }
  }

  /**
    * 启动Spark任务
    *
    * @param topics
    * @param kafkaParams
    * @return
    */
  private def startApp(topics: Array[String], kafkaParams: Map[String, Object]): StreamingContext = {
    val now = DateUtil.getFormatNowDate()
    // 获取spark运行时环境
    var (sparkContext, spark, ssc) = SparkUtil.getSparkStreamingRunTime(ConfigurationManager.getInt(Constants.SPARK_BATCH_DURATION))
    // 获取汇率表 并广播
    val exchangeMap = SparkUtil.getExchangeRateFromHive(spark)
    val exchangeMapBC = sparkContext.broadcast(exchangeMap)
    // 广播Kafka Producer
    val kafkaProducerBC: Broadcast[KafkaSink[String, String]] = sparkContext.broadcast(KafkaSink[String, String](kafkaParams))
    // 广播OTC产品代码 add 20201021
    val tfpCpdmMap = SparkUtil.getTfpCpdmFromHive(spark)
    val tfpCpdmMapBC = sparkContext.broadcast(tfpCpdmMap)
    // 获取输入流
    val inputStream = SparkUtil.getInputStreamFromKafkaByMysqlOffset(ssc, topics, kafkaParams)
    inputStream.foreachRDD(rdd => {
      // offset 信息
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // Rdd partition 分区数据处理
      val resultMap: Array[mutable.Map[String, Any]] = rdd
        .repartition(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM))
        .filter(message => StringUtils.isNotEmpty(message.value()))
        .mapPartitions(records => BaseRecordOperator.parsePartitionKafkaRecordsToBaseRecord(records))
        .filter(record => StringUtils.isNotEmpty(record._1))
        .groupByKey(ConfigurationManager.getInt(Constants.SPARK_CUSTOM_PARALLELISM) / 2)
        .mapPartitions(records => BaseRecordOperator.aggregatePartitionBaseRecords(records, exchangeMapBC,tfpCpdmMapBC, kafkaProducerBC))
        .collect()
      // 汇总计算各个分区计算结果
      val (tdrwt, tkhxx, tdrzjmx, tsscj,tfpywsqls,tfpywsqlscount, tgdzh, tjjzh, tzjzh, txhzh, tywqq) = aggregateAlllPartitionResult(resultMap)
      // 事务更新offset 及结果到Mysql
      DB.localTx(implicit session => {
        val timestamp = DateUtil.getNowTimestamp()
        flushTradeRealStateToDB(now, tdrwt, tsscj, timestamp)
        flushOtherRealStateToDB(now, tkhxx, tdrzjmx, tgdzh, tjjzh, tzjzh, txhzh, tfpywsqlscount, timestamp)
        flushOtcEntrRealStateToDB(now,tfpywsqls)/*OTC数据刷新到myssql*/
        flushOtherotcyybToDB(now,tywqq,timestamp)
        flushKafkaOffsetToDB(offsetRanges)
      })
      // 刷新 Redis 统计数据到Msyql
      FlushRedisToMysqlTask().run()
    })
    ssc.start()
    ssc
  }

  /**
    * 刷新Kafka消费的offset到数据库
    *
    * @param offsetRanges
    */
  private def flushKafkaOffsetToDB(offsetRanges: Array[OffsetRange])(implicit session: scalikejdbc.DBSession) = {
    var kafkaOffsets: List[List[Any]] = List()
    offsetRanges.foreach(offsetRange => {
      SQL(ConfigurationManager.getProperty(Constants.KAFKA_MYSQL_INSERT_OFFSET_SQL))
        .bind(offsetRange.topic, ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID)
          , offsetRange.partition, offsetRange.untilOffset).update().apply()
    })
  }

  /**
    * 刷新实时其他指标值到数据库(TKHXX,TDRZJMX)
    *
    * @param now
    * @param tkhxx
    * @param tdrzjmx
    * @param timestamp
    */
  private def flushOtherRealStateToDB(now: Int, tkhxx: mutable.Map[String, Int], tdrzjmx: mutable.Map[String, BigDecimal], tgdzh: mutable.Map[String, Int],  tjjzh: mutable.Map[String, Int],tzjzh: mutable.Map[String, Int],txhzh: mutable.Map[String, Int],tfpywsqlscount: mutable.Map[String,(Int,BigDecimal)],timestamp: Timestamp)(implicit session: scalikejdbc.DBSession) = {
    val new_gr_count = tkhxx.getOrElse(TkhxxOperator.GR, 0)
    val new_jg_count = tkhxx.getOrElse(TkhxxOperator.JG, 0)
    val zr_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZR, BigDecimal(0))
    val zc_sum = tdrzjmx.getOrElse(TdrzjmxOperator.ZJZC, BigDecimal(0))

    val new_zc_count = tgdzh.getOrElse("0",0)
    val find_count = tjjzh.getOrElse("8",0)
    val bnk_count = tzjzh.getOrElse("bankzh",0)
    val clos_count=txhzh.getOrElse("3",0)
    val tywsqls_count=tfpywsqlscount.getOrElse("count",(0,BigDecimal(0)))._1
    val value2 = tfpywsqlscount.getOrElse("count",(0,BigDecimal(0)))._2


  //   logger.warn(tywsqls_count+"  "+value2+"*****************flushOtherRealStateToDB****************************")
    if (new_jg_count != 0 || new_gr_count != 0  || zr_sum != 0 || zc_sum != 0 || new_zc_count != 0 || find_count !=0 || bnk_count !=0 || clos_count!=0 || tywsqls_count!=0  ) {
      var otherStates: List[List[Any]] = List()
      otherStates ::= (new_jg_count :: new_gr_count:: zr_sum :: zc_sum ::  new_zc_count :: find_count :: bnk_count :: clos_count :: tywsqls_count :: timestamp :: now :: new_jg_count :: new_gr_count:: zr_sum :: zc_sum::new_zc_count :: find_count :: bnk_count :: clos_count :: tywsqls_count :: timestamp :: now :: Nil)
      otherStates.foreach(otherState => {
        SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_OTHER_STATE_SQL))
          .bind(otherState: _*).update().apply()
      })
    }
  }

  private def flushOtherotcyybToDB(now: Int, tywqq: mutable.Map[String, Int],timestamp: Timestamp)(implicit session: scalikejdbc.DBSession) = {
    var otcywStates: List[List[Any]] = List()
    //    logger.warn("************flushOtherotcyybToDB***************")

    if(!tywqq.isEmpty){
      val key: collection.Set[String] = tywqq.keySet

      for(yyb <- key){
        var ywStates = tywqq.getOrElse(yyb,0)
        if(ywStates!=0){
          otcywStates ::= (yyb :: ywStates :: timestamp :: now :: yyb :: ywStates :: timestamp :: now :: Nil)
        }
      }
      if(!otcywStates.isEmpty){
        otcywStates.foreach(State=>{
          SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_OTCYWQQ_STATE_SQL))
            .bind(State: _*).update().apply()
        })
      }
    }
  }
  /*
  * 刷新OTC实时指标到mysql
  * @param now
  * @param tfpywsqls
  *TODO
  * */
  private def flushOtcEntrRealStateToDB(now:Int,tfpywsqls:mutable.Map[String,(Int,BigDecimal)])(implicit session: scalikejdbc.DBSession): Unit ={
   // logger.warn("**************OTC_TO_DB*************************************************")
    val ins_entr_tims = tfpywsqls.getOrElse("ins",(0,BigDecimal(0)))._1
    val crrc_entr_tims = tfpywsqls.getOrElse("crrc",(0,BigDecimal(0)))._1
    val not_crrc_entr_tims = tfpywsqls.getOrElse("not_crrc",(0,BigDecimal(0)))._1
    val ins_mtch_amt = tfpywsqls.getOrElse("ins",(0,BigDecimal(0)))._2
    val crrc_mtch_amt=tfpywsqls.getOrElse("crrc",(0,BigDecimal(0)))._2
    val not_crrc_mtch_amt=tfpywsqls.getOrElse("not_crrc",(0,BigDecimal(0)))._2
   // logger.warn(not_crrc_mtch_amt+"***********************222222**************************")
    if (!"0".equals(ins_entr_tims) || !"0".equals(crrc_entr_tims)  || !"0".equals(not_crrc_entr_tims) || !"0".equals(ins_mtch_amt) || !"0".equals(crrc_mtch_amt) || !"0".equals(not_crrc_mtch_amt)) {
    var otcStates : List[List[Any]] = List()
      otcStates ::=(ins_entr_tims::crrc_entr_tims::not_crrc_entr_tims::ins_mtch_amt::crrc_mtch_amt::not_crrc_mtch_amt::now::ins_entr_tims::crrc_entr_tims::not_crrc_entr_tims::ins_mtch_amt::crrc_mtch_amt::not_crrc_mtch_amt::now::Nil)
    otcStates.foreach(otcState => {
    //  logger.warn("**************OTC_TO_DB 2*************************************************")
      SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_OTC_STATE_SQL))
        .bind(otcState: _*).update().apply()
    })
    }
  }


  /**
    * 刷新实时交易指标值到数据库(TDRWT,TSSCJ)
    *
    * @param now
    * @param tdrwt
    * @param tsscj
    * @param timestamp
    */
  private def flushTradeRealStateToDB(now: Int, tdrwt: mutable.Map[(String, String), (Int,Int,Int,Int,Int, BigDecimal)], tsscj: mutable.Map[(String, String), (Int, BigDecimal, BigDecimal,BigDecimal)], timestamp: Timestamp)(implicit session: scalikejdbc.DBSession) = {
    var nowTradeStates: List[List[Any]] = List()
    if (!tdrwt.isEmpty || !tsscj.isEmpty) {
      val keys = tdrwt.keySet ++ tsscj.keySet;
      for ((yyb, channel) <- keys) {
        var (now_wt_tx_count, now_wt_tx_margin,now_wt_tx_stoc,now_wt_tx_fund,now_wt_tx_bond,now_wt_tx_sum) = tdrwt.getOrElse((yyb, channel), (0,0,0,0,0, BigDecimal(0)))
        var (cj_tx_count, cj_tx_sum, jyj,cjje_marg) = tsscj.getOrElse((yyb, channel), (0, BigDecimal(0), BigDecimal(0),BigDecimal(0)))
        //            val _channel = channels.get(channel).get
        if (now_wt_tx_count != 0 || cj_tx_count != 0) {
          nowTradeStates ::= (channel :: yyb :: now_wt_tx_count :: now_wt_tx_sum :: cj_tx_count :: cj_tx_sum :: jyj  :: timestamp :: now :: now_wt_tx_margin :: cjje_marg:: now_wt_tx_stoc::now_wt_tx_fund::now_wt_tx_bond ::channel :: yyb :: now_wt_tx_count :: now_wt_tx_sum :: cj_tx_count :: cj_tx_sum :: jyj :: timestamp :: now :: now_wt_tx_margin :: cjje_marg ::now_wt_tx_stoc::now_wt_tx_fund::now_wt_tx_bond ::Nil)
        }
      }
      if (!nowTradeStates.isEmpty) {
        nowTradeStates.foreach(tradeState => {
          SQL(ConfigurationManager.getProperty(Constants.MYSQL_UPSERT_NOW_TRADE_STATE_SQL))
            .bind(tradeState: _*).update().apply()
        })
      }
    }
  }

  /**
    * 汇总计算各个分区计算结果
    *
    * @param resultMap
    * @return
    */
  private def aggregateAlllPartitionResult(resultMap: Array[mutable.Map[String, Any]]) = {
    // 将Rdd计算结果收集到Driver端
    import scala.collection.mutable.Map
    // 计算委托相关的业务  委托笔数 两融委托笔数 委托金额  (yyb , channel)
    val tdrwt: mutable.Map[(String, String), (Int, Int,Int,Int,Int,BigDecimal)] = Map()
    // 新增客户数
    val tkhxx: mutable.Map[String, Int] = Map()
    // 计算客户转入转出
    val tdrzjmx: mutable.Map[String, BigDecimal] = Map()
    // 计算成交相关的业务 成交笔数  成交金额  佣金   (yyb , channel)
    val tsscj: mutable.Map[(String, String), (Int, BigDecimal, BigDecimal,BigDecimal)] = Map()
    //    OTC 业务申请流水统计
    val tfpywsqls:mutable.Map[String,(Int,BigDecimal)] = Map()
    val tfpywsqlscount:mutable.Map[String,(Int,BigDecimal)] = Map()
    //计算股东账户开户数
    val tgdzh: mutable.Map[String, Int] = Map()
    val tjjzh: mutable.Map[String, Int] = Map()
    val tzjzh: mutable.Map[String, Int] = Map()
    val txhzh: mutable.Map[String, Int] = Map()
    val tywqq: mutable.Map[String, Int] = Map()

    resultMap.foreach(resMap => {
      resMap.foreach(entry => {
        val resKey = entry._1
        val resValue = entry._2
        resKey match {
          case "tdrwt" => {
            resValue.asInstanceOf[mutable.Map[(String, String), (Int,Int,Int,Int,Int,BigDecimal)]].foreach(entry => {
              val key = entry._1
              val value = entry._2
              val oleValue = tdrwt.getOrElse(key, (0,0,0,0,0,BigDecimal(0)))
              tdrwt.put(key, (oleValue._1 + value._1, oleValue._2+value._2,oleValue._3 + value._3,oleValue._4+value._4,oleValue._5+value._5,oleValue._6+value._6))
            })
          }
          case "tsscj" => {
            resValue.asInstanceOf[mutable.Map[(String, String), (Int, BigDecimal, BigDecimal,BigDecimal)]].foreach(entry => {
              val key = entry._1
              val value = entry._2
              val oleValue = tsscj.getOrElse(key, (0, BigDecimal(0), BigDecimal(0),BigDecimal(0)))
              tsscj.put(key, (oleValue._1 + value._1, oleValue._2 + value._2, oleValue._3 + value._3,oleValue._4+value._4))
            })
          }
          case "tdrzjmx" => {
            resValue.asInstanceOf[mutable.Map[String, BigDecimal]].foreach(entry => {
              val key = entry._1
              val value = entry._2
              val oleValue = tdrzjmx.getOrElse(key, BigDecimal(0))
              tdrzjmx.put(key, oleValue + value)
            })
          }
          case "tkhxx" => {
            resValue.asInstanceOf[mutable.Map[String, Int]].foreach(entry => {
              val key = entry._1
              val value = entry._2
              val oleValue = tkhxx.getOrElse(key, 0)
              tkhxx.put(key, oleValue + value)
            })
          }
          //            OTC tfpywsqls 20201022新增
          /*
          * TODO*/
            case "tfpywsqls"=>{
              resValue.asInstanceOf[mutable.Map[String,(Int,BigDecimal)]].foreach(entry=>{
                val key=entry._1
                val value=entry._2
                val oldValue=tfpywsqls.getOrElse(key,(0,BigDecimal(0)))
           //     logger.warn("***********************agg  value"+value)
                tfpywsqls.put(key,(entry._2._1+oldValue._1,entry._2._2+oldValue._2))
              })
            }
          case "tfpywsqlscount"=>{
              //nnnnnnnnnn  logger.warn("ywsqls agg计算")
              resValue.asInstanceOf[mutable.Map[String,(Int,BigDecimal)]].foreach(entry=>{
                val key=entry._1
                val value=entry._2
                val oldValue=tfpywsqlscount.getOrElse(key,(0,BigDecimal(0)))
              //  logger.warn("***********************ywsqlsss取值结果"+key+"      "+value)
                tfpywsqlscount.put(key,(entry._2._1+oldValue._1,entry._2._2+oldValue._2))
              })
            }

          case "tgdzh" =>{
            resValue.asInstanceOf[mutable.Map[String, Int]].foreach(entry=>{
              val key = entry._1
              val value = entry._2
              val oldValue = tgdzh.getOrElse(key,0)
              tgdzh.put(key, oldValue+value)
            })
          }
          case "tjjzh" =>{
            //   logger.warn("计算逻辑")
            resValue.asInstanceOf[mutable.Map[String, Int]].foreach(entry=>{
              val key = entry._1
              val value = entry._2
              val oldValue = tjjzh.getOrElse(key,0)
              tjjzh.put(key, oldValue+value)
            })
          }
          case "tzjzh" =>{
            //   logger.warn("计算逻辑")
            resValue.asInstanceOf[mutable.Map[String, Int]].foreach(entry=>{
              val key = entry._1
              val value = entry._2
              val oldValue = tzjzh.getOrElse(key,0)
              tzjzh.put(key, oldValue+value)
            })
          }
          case "txhzh" =>{
            //logger.warn("txhzh   计算逻辑")
            resValue.asInstanceOf[mutable.Map[String, Int]].foreach(entry=>{
              val key = entry._1
              val value = entry._2
              val oldValue = txhzh.getOrElse(key,0)
              txhzh.put(key, oldValue+value)
            })
          }
          case "tywqq" =>{

            resValue.asInstanceOf[mutable.Map[String, Int]].foreach(entry=>{
              val key = entry._1
              val value = entry._2
              val oldValue = tywqq.getOrElse(key,0)
            //  logger.warn("ywqq666"+" "+value+" old"+oldValue+"yyb"+key)
              tywqq.put(key, oldValue+value)
            })
          }
        }
      })
    })
    (tdrwt, tkhxx, tdrzjmx, tsscj,tfpywsqls,tfpywsqlscount, tgdzh, tjjzh, tzjzh, txhzh, tywqq)
  }
}
