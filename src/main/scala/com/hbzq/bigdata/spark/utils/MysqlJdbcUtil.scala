package com.hbzq.bigdata.spark.utils

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
import org.apache.spark.sql.Row
import scalikejdbc._

/**
  * describe:
  * create on 2020/05/26
  *
  * Mysql数据库操作
  *
  * @author hqbhoho
  * @version [v1.0]
  *
  */
object MysqlJdbcUtil {

  var initState: Boolean = false

  /**
    * 初始化配置参数
    */
  def init(): Unit = {
    // initialize JDBC driver & connection pool
    Class.forName(ConfigurationManager.getProperty(Constants.MYSQL_JDBC_DRIVER))
    ConnectionPool.singleton(
      ConfigurationManager.getProperty(Constants.MYSQL_JDBC_URL),
      ConfigurationManager.getProperty(Constants.MYSQL_JDBC_USERNAME),
      ConfigurationManager.getProperty(Constants.MYSQL_JDBC_PASSWORD)
    )
    initState = true
  }

  /**
    * 执行查询操作
    *
    * @param sql
    * @return
    */
  def executeQuery(sql: String): List[Row] = {
    if (!initState) init()
    try {
      DB.readOnly { implicit session =>
        SQL(sql).map(rs => Row(rs.toMap().values)).list.apply()
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        List()
      }
    }
  }

  /**
    * 执行批量更新操作
    *
    * @param sql
    * @return
    */
  def executeUpdate(sql: String, params: List[Any]): Unit = {
    if (!initState) init()
    try {
      DB.localTx(implicit session => {
        SQL(sql).bind(params: _*).update().apply()
      })
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

  }

  /**
    * 批量更新
    *
    * @param sql
    * @param paramsList
    */
  def executeBatchUpdate(sql: String, paramsList: List[List[Any]]): Unit = {
    if (!initState) init()
    try {
      DB.localTx(implicit session => {
        paramsList.foreach(params => {
          SQL(sql).bind(params: _*).update().apply()
        })
      })
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }
}
