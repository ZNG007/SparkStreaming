package com.hbzq.bigdata.spark.utils

import java.util.concurrent.TimeUnit

import com.google.common.cache._
import com.hbzq.bigdata.spark.config.Constants
import com.hbzq.bigdata.spark.domain.TdrwtRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, MasterNotRunningException, TableName}
import org.apache.hadoop.hbase.client.{Result, _}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.reflect.runtime.universe._

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
    val table = getHbaseClient(tableName)
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
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("KHH"), Bytes.toBytes(tdrwtRecord.khh))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTH"), Bytes.toBytes(tdrwtRecord.wth))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("YYB"), Bytes.toBytes(tdrwtRecord.yyb))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("BZ"), Bytes.toBytes(tdrwtRecord.bz))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("CHANNEL"), Bytes.toBytes(tdrwtRecord.channel))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTSL"), Bytes.toBytes(tdrwtRecord.wtsl.toString))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("WTJG"), Bytes.toBytes(tdrwtRecord.wtjg.toString))
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

  def parseHBaseRecordToMap(record: Result, columnFamily: String): scala.collection.mutable.Map[String, String] = {
    var res = scala.collection.mutable.Map[String, String]()
    record.rawCells().foreach(cell => {
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      res += (key -> value)
    })
    res
  }

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
