package com.hbzq.bigdata.spark.utils

import java.util
import java.util.concurrent.TimeUnit

import com.google.common.cache._
import com.hbzq.bigdata.spark.config.Constants
import com.hbzq.bigdata.spark.domain.{TFPYwsqlsRecord, TdrwtRecord, TkhxxRecord, TywqqRecord}
import com.hbzq.bigdata.spark.utils.HBaseUtil.parseHBaseAllRecordToMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, MasterNotRunningException, TableName}
import org.apache.hadoop.hbase.client.{Result, _}
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.TableReducer
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{NullWritable, Text}
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.reflect.runtime.universe._
import org.apache.log4j.Logger

/**
  * describe:
  * create on 2020/06/11
  *
  * @author hqbhoho
  * @version [v1.0] 
  *
  */

object CacheHbaseClient {
  private val loader = new CacheLoader[String, Table]() {
    def load(tableName: String): Table = {
      HBaseUtil.getNewHbaseClient(tableName)
    }
  }

  private val listener = new RemovalListener[String, Table]() {
    def onRemoval(rn: RemovalNotification[String, Table]): Unit = {
      if (rn != null && rn.getValue != null) {
        rn.getValue
      }
    }
  }

  val cache: LoadingCache[String, Table] = CacheBuilder
    .newBuilder()
    .expireAfterAccess(3600, TimeUnit.SECONDS)
    .removalListener(listener)
    .build(loader)
}

object HBaseUtil {

  private[this] val logger = Logger.getLogger(HBaseUtil.getClass)

  //hbase 连接的参数
  val config: Configuration = HBaseConfiguration.create()

  def getNewHbaseClient(tableName: String): Table = {
    var table: Table = null
    try {
      HBaseAdmin.checkHBaseAvailable(config)
      val conn = ConnectionFactory.createConnection(config)
      table = conn.getTable(TableName.valueOf(tableName))

    }
    catch {
      case e: MasterNotRunningException => {

      }
    }
    table
  }

  def getHbaseClient(tableName: String): Table = {
    CacheHbaseClient.cache.get(tableName)
  }


  /**
    * HBase 插入单列数据
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @param column
    * @param value
    */
  def insertSingleColMessageToHBase(tableName: String, rowkey: String, columnFamily: String, column: String, value: String) = {
    val table = getHbaseClient(tableName)
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
  }


  /**
    * 批量插入
    *
    * @param tableName
    * @param puts
    */
  def BatchMultiColMessageToHBase(tableName: String, puts: List[Put]) {
     logger.warn("ywqqhbasetable "+tableName+" puts "+puts)
    val table = getHbaseClient(tableName)
    //table.setWriteBufferSize(5*1024*1024)
    table.put(puts.asJava)
  }

  /**
    * 转换
    *
    * @param tdrwtRecord
    * @return
    */
  def parseTdrwtToPut(tdrwtRecord: TdrwtRecord, columnFamily: String): Put = {
    val rowkey = getRowKeyFromInteger(tdrwtRecord.wth.toInt)
//    val rowkey1: String = rowkey
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("KHH"), Bytes.toBytes(tdrwtRecord.khh))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTH"), Bytes.toBytes(tdrwtRecord.wth))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("YYB"), Bytes.toBytes(tdrwtRecord.yyb))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("BZ"), Bytes.toBytes(tdrwtRecord.bz))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CHANNEL"), Bytes.toBytes(tdrwtRecord.channel))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTSL"), Bytes.toBytes(tdrwtRecord.wtsl.toString))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTJG"), Bytes.toBytes(tdrwtRecord.wtjg.toString))
    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("CXBZ"),Bytes.toBytes(tdrwtRecord.cxbz.toString))
    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("JSLX"),Bytes.toBytes(tdrwtRecord.jslx.toString))
    put
  }
  def parseTkhxxToPut(tkhxxRecord: TkhxxRecord, columnFamily: String): Put = {
    val rowkey = tkhxxRecord.khh.reverse
    val put = new Put(Bytes.toBytes(rowkey))
    logger.warn("ywqqputs4 "+tkhxxRecord.khh+" content "+tkhxxRecord)
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("KHH"), Bytes.toBytes(tkhxxRecord.khh))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("XHRQ"), Bytes.toBytes(tkhxxRecord.xhrq.toString))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("KHZT"), Bytes.toBytes(tkhxxRecord.khzt))
    put
  }
  def parseTywqqToPut(tywqqRecord: TywqqRecord, columnFamily: String): Put = {
   // val rowkey = getRowKeyFromInteger(tywqqRecord.id.toInt)
    val rowkey = tywqqRecord.id.reverse
  //gg  val rowkey1: String = rowkey
  //  logger.warn("ywqqputs3 "+rowkey+"columnFamily "+columnFamily)
    val put = new Put(Bytes.toBytes(rowkey))
    logger.warn("ywqqputs4 "+tywqqRecord.id+" content "+tywqqRecord)
   // !"".equalsIgnoreCase(TywqqRecord.khh)
//    if(
//      !"".equalsIgnoreCase(tywqqRecord.yyb) &&
//      !"".equalsIgnoreCase(tywqqRecord.clzt)
//    ){
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("ID"), Bytes.toBytes(tywqqRecord.id))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("KHH"), Bytes.toBytes(tywqqRecord.khh))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("YYB"), Bytes.toBytes(tywqqRecord.yyb))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("SQRQ"), Bytes.toBytes(tywqqRecord.sqrq.toString))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CLZT"), Bytes.toBytes(tywqqRecord.clzt))
//    }
     put
  }
  def parseTywsqlsToPut(tywsqlsRecord: TFPYwsqlsRecord, columnFamily: String): Put = {
    val rowkey = getRowKeyFromInteger(tywsqlsRecord.wth.toInt)
   //val rowkey = tywsqlsRecord.jyzh.reverse
  //gg  val rowkey1: String = rowkey
  //  logger.warn("ywsqlsputs33 "+rowkey+"columnFamily "+columnFamily)
    val put = new Put(Bytes.toBytes(rowkey))
    logger.warn("ywsqlsputs44 "+tywsqlsRecord.wth+" clzt "+tywsqlsRecord.clzt+" cljg "+tywsqlsRecord.cljg+" ywdm "+tywsqlsRecord.ywdm+" cpfl "+tywsqlsRecord.cpfl)

      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("KHH"), Bytes.toBytes(tywsqlsRecord.khh))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTH"), Bytes.toBytes(tywsqlsRecord.wth))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("YWDM"), Bytes.toBytes(tywsqlsRecord.ywdm))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CLZT"), Bytes.toBytes(tywsqlsRecord.clzt))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CLJG"), Bytes.toBytes(tywsqlsRecord.cljg))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CPFL"), Bytes.toBytes(tywsqlsRecord.cpfl))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("YWJE"), Bytes.toBytes(tywsqlsRecord.ywje.toString()))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("DJRQ"), Bytes.toBytes(tywsqlsRecord.djrq))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CPID"), Bytes.toBytes(tywsqlsRecord.cpid))
     put
  }

  def parseTsscjToPut(tuple3: (String ,String , BigDecimal), columnFamily: String): Put = {
    val rowkey = tuple3._1
   //val rowkey = tywsqlsRecord.jyzh.reverse
  //gg  val rowkey1: String = rowkey
  //  logger.warn("Tsscjputs33 "+rowkey+"columnFamily "+columnFamily)
    val put = new Put(Bytes.toBytes(rowkey))
  //  logger.warn("Tsscjputs44 "+"content "+tuple3)
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTH"), Bytes.toBytes(tuple3._1))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("GDH"), Bytes.toBytes(tuple3._2))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CJJE"), Bytes.toBytes(tuple3._3.toString()))
    put
  }
  def parseTcjjeToPut(tuple: (String,String, BigDecimal), columnFamily: String): Put = {
    val rowkey = tuple._1
    //val rowkey = tywsqlsRecord.jyzh.reverse
    //gg  val rowkey1: String = rowkey
  //  logger.warn("cjjeputs33 "+rowkey+"columnFamily "+columnFamily)
    val put = new Put(Bytes.toBytes(rowkey))
 //   logger.warn("cjjeputs44 "+tuple._1+"content "+tuple)
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTH"), Bytes.toBytes(tuple._1))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("GDH"), Bytes.toBytes(tuple._2))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CJJE"), Bytes.toBytes(tuple._3.toString()))
    put
  }
  /**
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @param columns
    */
  def insertMultiColMessageToHBase(tableName: String, rowkey: String, columnFamily: String, columns: Map[String, Any]): Unit = {
    val table = getHbaseClient(tableName)
    val put = new Put(Bytes.toBytes(rowkey))
    for (entry <- columns) {
      val key = entry._1
      val value = entry._2
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(value.toString))
    }
    table.put(put)
  }

  /**
    * 批量get数据
    *
    * @param tableName
    * @param columnFamily
    * @param onlyWths
    * @return
    */
  def getRecordsFromHBaseByKeys(tableName: String, columnFamily: String, onlyWths: collection.Set[String]): Map[String, Map[String, String]] = {
    import scala.collection.mutable.Map
    var gets: List[Get] = List()
    onlyWths.map(wth => getRowKeyFromInteger(wth.toInt))
      .foreach(rowkey => {
        val get = new Get(rowkey.getBytes)
        gets ::= get
      })
    val table = getHbaseClient(tableName)
    val results: List[Result] = table.get(gets.asJava).toList
    var res: Map[String, Map[String, String]] = Map()
    results.foreach(record => {
      val hbaseData = parseHBaseRecordToMap(record, columnFamily)
      val wth = hbaseData.get("WTH").getOrElse("0")
      res.put(wth, hbaseData)
    })
    res
  }
  def getTkhxxRecordsFromHBaseByKeys(tableName: String, columnFamily: String, hbaseTkhxxReords: collection.Set[String]): Map[String, Map[String, String]] = {
    import scala.collection.mutable.Map
    var gets: List[Get] = List()
    hbaseTkhxxReords.map(khh =>khh.reverse)
      .foreach(rowkey => {
        val get = new Get(rowkey.getBytes)
        gets ::= get
      })
    val table = getHbaseClient(tableName)
    val results: List[Result] = table.get(gets.asJava).toList
    var res: Map[String, Map[String, String]] = Map()
    results.foreach(record => {
      val hbaseData = parseHBaseRecordToMap(record, columnFamily)
      val khh = hbaseData.get("KHH").getOrElse("0")
      res.put(khh, hbaseData)
    })
    res
  }
  def getTywqqRecordsFromHBaseByKeys(tableName: String, columnFamily: String, tywqqUpdateWths: collection.Set[String]): Map[String, Map[String, String]] = {
    import scala.collection.mutable.Map
    var gets: List[Get] = List()
    tywqqUpdateWths.map(id => id.reverse)
      .foreach(rowkey => {
        val get = new Get(rowkey.getBytes)
        gets ::= get
      })
    val table = getHbaseClient(tableName)
    val results: List[Result] = table.get(gets.asJava).toList
    var res: Map[String, Map[String, String]] = Map()
    results.foreach(record => {
      val hbaseData = parseHBaseRecordToMap(record, columnFamily)
      val id = hbaseData.get("ID").getOrElse("0")
      res.put(id, hbaseData)
    })
    res
  }
//  def getRecordsFromywqqALLTableScanHBaseByKeys(tableName: String, columnFamily: String): Map[String, Map[String, String]] = {
//    import scala.collection.mutable.Map
//    val table = getHbaseClient(tableName)
//    val scan: Scan = new Scan()
//    scan.addFamily(Bytes.toBytes(columnFamily))
//  //  TableMapReduceUtil
//    val scanner: ResultScanner = table.getScanner(scan)
//    var res: Map[String, Map[String, String]] = Map()
//    val it: util.Iterator[Result] = scanner.iterator()
//
//    TableMapReduceUtil.initTableReduceJob()
//
//    while (it.hasNext) {
//      val next: Result = it.next()
//      val hbaseData: mutable.Map[String, String] = parseHBaseAllRecordToMap(next,columnFamily)
//      val rowid = hbaseData.get("ID").getOrElse("0")
//      res.put(rowid, hbaseData)
//    }
//    scanner.close()
//    res
//  }






  def getRecordsFromywqqALLHBaseByKeys(tableName: String, columnFamily: String): Map[String, Map[String, String]] = {
    import scala.collection.mutable.Map
    val table = getHbaseClient(tableName)
    val scan: Scan = new Scan()
    scan.addFamily(Bytes.toBytes(columnFamily))
    val scanner: ResultScanner = table.getScanner(scan)
    var res: Map[String, Map[String, String]] = Map()
    //    ResultScanner.forEach( record => {
    //      val hbaseData: mutable.Map[String, String] = parseHBaseRecordToMap(record, columnFamily)
    //      val rowid = hbaseData.get("rowid").getOrElse("0")
    //      logger.warn("ywsqlshbasefrom0 "+" rowid "+rowid+" hbaseData "+hbaseData)
    //      res.put(rowid, hbaseData)
    //    })

    val it: util.Iterator[Result] = scanner.iterator()
    while (it.hasNext) {
      val next: Result = it.next()
      val hbaseData: mutable.Map[String, String] = parseHBaseAllRecordToMap(next,columnFamily)
      val rowid = hbaseData.get("ID").getOrElse("0")
     // logger.warn("ywqqhbasefrom0 ")
      res.put(rowid, hbaseData)
      //      for (kv <- next.raw()) {
      //
      //      }
      //      var res = scala.collection.mutable.Map[String, String]()
      //      val cells = next.rawCells()
      //      for(cell<-cells){
      //        println(new String(cell.getRowArray)+" row")
      //        println(new String(cell.getFamilyArray))
      //        println(new String(cell.getQualifierArray))
      //        println(new String(cell.getValueArray))
      //        println(cell.getTimestamp)
      //        println("---------------------")
      //        logger.warn("hbasexinxi "+new String(cell.getRowArray)+" "+new String(cell.getFamilyArray)
      //        +" "+new String(cell.getQualifierArray)+" "+new String(cell.getValueArray) )
      //        val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      //        val value = Bytes.toString(CellUtil.cloneValue(cell))
      //        res += (key -> value)
      //      }

      //      def parseHBaseRecordToMap(record: Result, columnFamily: String): scala.collection.mutable.Map[String, String] = {
      //        var res = scala.collection.mutable.Map[String, String]()
      //        record.rawCells().foreach(cell => {
      //          val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      //          val value = Bytes.toString(CellUtil.cloneValue(cell))
      //          res += (key -> value)
      //        })
      //        res
      //      }
    }

    scanner.close()
    res
  }
  //      var res = scala.collection.mutable.Map[String, String]()
  //         // res.put("rowid",Bytes.toString(record.getRow))
  //          record.listCells().forEach(cell=>{
  //            val key = Bytes.toString(CellUtil.cloneQualifier(cell))
  //            val value = Bytes.toString(CellUtil.cloneValue(cell))
  //            res += (key -> value)
  //          })
  def getRecordsFromywsqlsALLHBaseByKeys(tableName: String, columnFamily: String): Map[String, Map[String, String]] = {
    //  import scala.collection.mutable.Map

    val table = getHbaseClient(tableName)
    val scan: Scan = new Scan()
    scan.addFamily(Bytes.toBytes(columnFamily))
    val scanner: ResultScanner = table.getScanner(scan)
    var res: Map[String, Map[String, String]] = Map()
    //    ResultScanner.forEach( record => {
    //      val hbaseData: mutable.Map[String, String] = parseHBaseRecordToMap(record, columnFamily)
    //      val rowid = hbaseData.get("rowid").getOrElse("0")
    //      logger.warn("ywsqlshbasefrom0 "+" rowid "+rowid+" hbaseData "+hbaseData)
    //      res.put(rowid, hbaseData)
    //    })

    val it: util.Iterator[Result] = scanner.iterator()
    while (it.hasNext) {
      val next: Result = it.next()
      val hbaseData: mutable.Map[String, String] = parseHBaseAllRecordToMap(next,columnFamily)
      val rowid = hbaseData.get("WTH").getOrElse("0")
      //  logger.warn("ywsqlshbase ")
      res.put(rowid, hbaseData)
    }
    scanner.close()
    res
  }

  def getywqqRecordsFromHBaseByKeys(tableName: String, columnFamily: String , tywqqInsertWths: collection.Set[String]): Map[String, Map[String, String]] = {
    import scala.collection.mutable.Map
    var gets: List[Get] = List()
    tywqqInsertWths.map(rowid => rowid.reverse)
      .foreach(rowkey => {
        val get = new Get(rowkey.getBytes)
        gets ::= get
      })
    val table = getHbaseClient(tableName)
    val results: List[Result] = table.get(gets.asJava).toList
    var res: Map[String, Map[String, String]] = Map()
    results.foreach(record => {
      val hbaseData = parseHBaseRecordToMap(record, columnFamily)
      val rowid = hbaseData.get("rowid").getOrElse("0")
      res.put(rowid, hbaseData)
    })
    res
  }
  def getywsqlsRecordsFromHBaseByKeys(tableName: String, columnFamily: String , tywsqlsInsertWths: collection.Set[String]): Map[String, Map[String, String]] = {
    import scala.collection.mutable.Map
    var gets: List[Get] = List()
    tywsqlsInsertWths.map(rowid => rowid.reverse)
      .foreach(rowkey => {
        val get = new Get(rowkey.getBytes)
        gets ::= get
      })
    val table = getHbaseClient(tableName)
    val results: List[Result] = table.get(gets.asJava).toList
    var res: Map[String, Map[String, String]] = Map()
    results.foreach(record => {
      val hbaseData = parseHBaseRecordToMap(record, columnFamily)
      val rowid = hbaseData.get("rowid").getOrElse("0")
      res.put(rowid, hbaseData)
    })
    res
  }

  def parseHBaseRecordToMap(record: Result, columnFamily: String): scala.collection.mutable.Map[String, String] = {
    var res = scala.collection.mutable.Map[String, String]()
    record.rawCells().foreach(cell => {
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      res += (key -> value)
    })
    res
  }
  def parseHBaseAllRecordToMap(next: Result, columnFamily: String): scala.collection.mutable.Map[String, String] = {
    var res = scala.collection.mutable.Map[String, String]()
    val cells = next.rawCells()
    for(cell<-cells){
      //      println(new String(cell.getRowArray)+" row")
      //      println(new String(cell.getFamilyArray))
      //      println(new String(cell.getQualifierArray))
      //      println(new String(cell.getValueArray))
      //      println(cell.getTimestamp)
      //      println("---------------------")
      //      logger.warn("hbasexinxi "+new String(cell.getRowArray)+" "+new String(cell.getFamilyArray)
      //        +" "+new String(cell.getQualifierArray)+" "+new String(cell.getValueArray) )
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      res += (key -> value)
    }
    res
  }


//  def HDFSToHbaseReducer extends TableReducer(Text, NullWritable, NullWritable){
//
//    protected void reduce(Text key, Iterable<NullWritable> values,Context context)
//    throws IOException, InterruptedException {
//
//      String[] split = key.toString().split(",");
//
//      Put put = new Put(split[0].getBytes());
//
//      put.addColumn("info".getBytes(), "name".getBytes(), split[1].getBytes());
//      put.addColumn("info".getBytes(), "sex".getBytes(), split[2].getBytes());
//      put.addColumn("info".getBytes(), "age".getBytes(), split[3].getBytes());
//      put.addColumn("info".getBytes(), "department".getBytes(), split[4].getBytes());
//
//      context.write(NullWritable.get(), put);
//
//    }
//
//  }

  /**
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @param column
    */
  def getMessageStrFromHBaseBySingleCol(tableName: String, rowkey: String, columnFamily: String, column: String): String = {
    val table = getHbaseClient(tableName)
    val get = new Get(rowkey.getBytes)
    val res = table.get(get).getValue(columnFamily.getBytes(), column.getBytes())
    if (res == null || res.length == 0) {
      return ""
    }
    new String(res)
  }

  /**
    * 获取当行数据所有列
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @return
    */
  def getMessageStrFromHBaseByAllCol(tableName: String, rowkey: String, columnFamily: String): Map[String, String] = {
    val table = getHbaseClient(tableName)
    val get = new Get(rowkey.getBytes)
    var res = Map[String, String]()
    table.get(get).rawCells().foreach(cell => {
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      res += (key -> value)
    })
    res
  }


  /**
    * int类型递增序列的rowkey生成
    *
    * @param num
    */
  def getRowKeyFromInteger(num: Int): String = {
    (Integer.MAX_VALUE - num).toString.reverse
  }
}
