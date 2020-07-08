package com.hbzq.bigdata.spark.utils

import java.util.concurrent.TimeUnit

import com.google.common.cache._
import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, MasterNotRunningException, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

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
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @param columns
    */
  def insertMultiColMessageToHBase(tableName: String, rowkey: String, columnFamily: String,columns: Map[String,Any]): Unit ={
    val table = getHbaseClient(tableName)
    val put = new Put(Bytes.toBytes(rowkey))
    for(entry <- columns){
      val key = entry._1
      val value = entry._2
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key),Bytes.toBytes(value.toString))
    }
    table.put(put)
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
    if(res == null || res.length == 0){
      return ""
    }
    new String(res)
  }

  /**
    * 获取当行数据所有列
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @return
    */
  def getMessageStrFromHBaseByAllCol(tableName: String, rowkey: String, columnFamily: String): Map[String,String] = {
    val table = getHbaseClient(tableName)
    val get = new Get(rowkey.getBytes)
    var res = Map[String,String]()
    table.get(get).rawCells().foreach(cell=>{
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
