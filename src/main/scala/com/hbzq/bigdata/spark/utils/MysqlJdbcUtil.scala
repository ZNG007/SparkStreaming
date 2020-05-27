package com.hbzq.bigdata.spark.utils

import com.hbzq.bigdata.spark.config.{ConfigurationManager, Constants}
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
  def executeQuery(sql: String): List[Map[String, Any]] = {
    if (!initState) init()
    DB.readOnly { implicit session =>
      SQL(sql).map(_.toMap()).list.apply()
    }
  }

  /**
    * 执行更新操作
    *
    * @param sql
    * @return
    */
  def executeUpdate(sql: String): Unit = {
    if (!initState) init()
    DB.localTx(implicit session => {
      SQL(sql).update().apply()
    })
  }
}
