package com.hbzq.bigdata.spark.config

/**
  * describe:
  * create on 2020/05/27
  *
  *  配置常量
  *
  * @author hqbhoho
  * @version [v1.0]
  *
  */
object Constants {
  // 常规配置
  val DELIMITER = ","
  val SPARK_BATCH_DURATION="spark.batch.duration"
  val SPARK_CUSTOM_PARALLELISM="spark.custom.parallelism"

  // MYSQL JDBC相关常量
  val MYSQL_JDBC_DRIVER = "mysql.jdbc.driver"
  val MYSQL_JDBC_URL = "mysql.jdbc.url"
  val MYSQL_JDBC_USERNAME = "mysql.jdbc.username"
  val MYSQL_JDBC_PASSWORD = "mysql.jdbc.password"

  // 交易通道分类标识
  val RULES_LIST = "rule.list"
  val RULE_TX_CHANNEL_CLASSIFY="tx_channel"
  // Kafka 配置参数
  val KAFKA_TOPICS = "kafka.topics"
  val KAFKA_BOOTSTRAP_SERVERS="kafka.bootstrap.servers"
  val KAFKA_GROUP_ID="kafka.group.id"
  val KAFKA_AUTO_OFFSET_RESET="kafka.auto.offset.reset"
  val KAFKA_MYSQL_QUERY_OFFSET_SQL="kafka.mysql.query.offset.sql"
  val KAFKA_MYSQL_INSERT_OFFSET_SQL="kafka.mysql.insert.offset.sql"
  val KAFKA_TOPIC_TDRWT_NAME="kafka.topic.tdrwt.name"
  val KAFKA_TOPIC_TDRZJMX_NAME="kafka.topic.tdrzjmx.name"
  val KAFKA_TOPIC_TSSCJ_NAME="kafka.topic.tsscj.name"
  val KAFKA_TOPIC_TKHXX_NAME="kafka.topic.tkhxx.name"
  val KAFKA_TOPIC_TRADE_MONITOR_SLOW_NAME="kafka.topic.monitor.slow.name"
  // Redis
  val REDIS_HOSTS = "redis.hosts"
  val REDIS_MASTER = "redis.master"
  val REDIS_TIMEOUT = "redis.timeout"
  val REDIS_MAX_TOTAL = "redis.max.total"
  val REDIS_MAX_IDLE = "redis.max.idle"
  val REDIS_MIN_IDLE = "redis.min.total"
  val REDIS_TEST_ON_BORROW = "redis.test.on.borrow"
  val REDIS_TEST_ON_RETURN = "redis.test.on.return"
  val REDIS_MAX_WAIT_MILLIS = "redis.max.wait.millis"
  // Redis key expire
  val REDIS_KEY_DEL_PATTERN="redis.key.del.pattern"
  val REDIS_KEY_DEL_SCHEDULE_INTERVAL ="redis.key.del.schedule.interval"
  val REDIS_KEY_DEL_MACTH_HOUR ="redis.key.del.match.hour"
  val REDIS_VALUE_DOUBLE_TO_LONG="redis.value.double.to.long"
  val FLUSH_REDIS_TO_MYSQL_SCHEDULE_INTERVAL ="redis.flush.mysql.schedule.interval"

  // HBase
  val HBASE_TDRWT_WTH_TABLE="hbase.tdrwt.wth.table"
  val HBASE_WTH_INFO_FAMILY_COLUMNS="hbase.wth.info.family.columns"


  val HBASE_TYWQQ_ROWID_TABLE="hbase.tywqq.rowid.table"
  val HBASE_ROWID_INFO_FAMILY_COLUMNS="hbase.rowid.info.family.columns"

  val HBASE_Tywsqls_ROWID_TABLE="hbase.tywsqls.rowid.table"
  val HBASE_ROWID_INFOF_FAMILY_COLUMNS="hbase.rowid.infof.family.columns"

  // exchange rate sql
  val DIM_EXCHANGE_RATE_SQL = "hive.dim.exchange.rate.sql"
  val DIM_TFP_CPDM_SQL="hive.dim.tfp.cpdm.sql"
  val MYSQL_UPSERT_NOW_TRADE_STATE_SQL = "mysql.upsert.now.trade.state.sql"
  val MYSQL_UPSERT_NEXT_TRADE_STATE_SQL = "mysql.upsert.next.trade.state.sql"
  val FLUSH_REDIS_TO_MYSQL_TRADE_STATE_KHH_SQL = "flush.redis.to.mysql.trade.state.khh.sql"
  val MYSQL_UPSERT_OTHER_STATE_SQL = "mysql.upsert.other.state.sql"
  val MYSQL_UPSERT_OTCYWQQ_STATE_SQL="mysql.upsert.otcywqq.state.sql"
  val MYSQL_NEXT_TRADE_DAY_QUERY_SQL = "mysql.next.trade.day.query.sql"
  val MYSQL_UPSERT_OTC_STATE_SQL="mysql.upsert.otc.state.sql"

  // stop job
  val APP_STOP_HOUR ="app.stop.hour"

  // tsscj 未关联次数阀值
  val TSSCJ_LIFE_CYCLE_THRESHOLD="tsscj.life.cycle.threshold"

}
