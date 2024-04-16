/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.server.metadata.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.Locale

import scala.collection.mutable.ListBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.annotations.VisibleForTesting
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.metadata.WhitelistStore
import org.apache.kyuubi.server.metadata.api.{Blacklist, BWlistFilter, Whitelist}
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType._
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._

class BWlistStore(conf: KyuubiConf) extends WhitelistStore with Logging {
  import BWlistStore._

  private val dbType = DatabaseType.withName(conf.get(METADATA_STORE_JDBC_DATABASE_TYPE))
  private val driverClassOpt = conf.get(METADATA_STORE_JDBC_DRIVER)
  private val driverClass = dbType match {
    case DERBY => driverClassOpt.getOrElse("org.apache.derby.jdbc.AutoloadedDriver")
    case MYSQL => driverClassOpt.getOrElse("com.mysql.jdbc.Driver")
    case CUSTOM => driverClassOpt.getOrElse(
        throw new IllegalArgumentException("No jdbc driver defined"))
  }

  private val datasourceProperties =
    JDBCMetadataStoreConf.getMetadataStoreJDBCDataSourceProperties(conf)
  private val hikariConfig = new HikariConfig(datasourceProperties)
  hikariConfig.setDriverClassName(driverClass)
  hikariConfig.setJdbcUrl(conf.get(METADATA_STORE_JDBC_URL))
  hikariConfig.setUsername(conf.get(METADATA_STORE_JDBC_USER))
  hikariConfig.setPassword(conf.get(METADATA_STORE_JDBC_PASSWORD))
  hikariConfig.setPoolName("jdbc-metadata-store-pool")

  @VisibleForTesting
  private[kyuubi] val hikariDataSource = new HikariDataSource(hikariConfig)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  override def close(): Unit = {
    hikariDataSource.close()
  }

  override def getBlacklist: Seq[Blacklist] = {
    val query = s"SELECT * FROM $BLACKLIST_TABLE"
    withConnection() { connection =>
      withResultSet(connection, query) { rs =>
        buildBlacklist(rs)
      }
    }
  }

  override def hitInBlacklist(
      username: String = null,
      requestName: String = null,
      region: String = null,
      engineType: String = null,
      cluster: String = null,
      service: String = null,
      fuzzyMatch: Boolean = true): Seq[Blacklist] = {
    val filter = BWlistFilter(
      username = username,
      requestName = requestName,
      region: String,
      engineType = engineType,
      cluster = cluster,
      service = service)
    val (query, params) = if (fuzzyMatch) {
      buildQueryByFuzzyMatch(filter, BLACKLIST_TABLE)
    } else {
      buildQueryByExactMatch(filter, BLACKLIST_TABLE)
    }
    try {
      withConnection() { connection =>
        withResultSet(connection, query, params: _*) { rs =>
          buildBlacklist(rs)
        }
      }
    } catch {
      case e: Throwable =>
        error("HitInBlacklist error: ", e)
        null
    }
  }

  override def getWhitelist: Seq[Whitelist] = {
    val query = s"SELECT * FROM $WHITELIST_TABLE"
    withConnection() { connection =>
      withResultSet(connection, query) { rs =>
        buildWhitelist(rs)
      }
    }
  }

  override def hitInWhitelist(
      username: String = null,
      requestName: String = null,
      region: String = null,
      engineType: String = null,
      cluster: String = null,
      service: String = null,
      fuzzyMatch: Boolean = true): Seq[Whitelist] = {
    val filter = BWlistFilter(
      username = username,
      requestName = requestName,
      region = region,
      engineType = engineType,
      cluster = cluster,
      service = service)
    val (query, params) = if (fuzzyMatch) {
      buildQueryByFuzzyMatch(filter, WHITELIST_TABLE)
    } else {
      buildQueryByExactMatch(filter, WHITELIST_TABLE)
    }
    try {
      withConnection() { connection =>
        withResultSet(connection, query, params: _*) { rs =>
          buildWhitelist(rs)
        }
      }
    } catch {
      case e: Throwable =>
        error("HitInWhitelist error: ", e)
        null
    }
  }

  def buildQueryByFuzzyMatch(filter: BWlistFilter, table: String): (String, ListBuffer[Any]) = {
    val queryBuilder = new StringBuilder
    val params = ListBuffer[Any]()
    queryBuilder.append(s"SELECT * FROM $table")

    val whereConditions = ListBuffer[String]()
    Option(filter.username).foreach { username =>
      whereConditions += " username IN (?, 'all')"
      params += username
    }
    Option(filter.requestName).foreach { requestName =>
      whereConditions += " request_name IN (?, 'all')"
      params += requestName
    }
    Option(filter.region).foreach { region =>
      whereConditions += " region IN (?, 'all')"
      params += region
    }
    Option(filter.engineType).foreach { engineType =>
      whereConditions += " LOWER(engine_type) IN (?, 'all')"
      params += engineType.toLowerCase(Locale.ROOT)
    }
    Option(filter.cluster).foreach { cluster =>
      whereConditions += " cluster IN (?, 'all')"
      params += cluster
    }
    Option(filter.service).foreach { service =>
      whereConditions += " service IN (?, 'all')"
      params += service
    }
    if (whereConditions.nonEmpty) {
      queryBuilder.append(whereConditions.mkString(" WHERE ", " AND ", " "))
    }
    queryBuilder.append(" ORDER BY username ")
    val query = queryBuilder.toString()
    logger.debug(query)
    logger.debug(params.toString())
    (query, params)
  }

  def buildQueryByExactMatch(filter: BWlistFilter, table: String): (String, ListBuffer[Any]) = {
    val queryBuilder = new StringBuilder
    val params = ListBuffer[Any]()
    queryBuilder.append(s"SELECT * FROM $table")

    val whereConditions = ListBuffer[String]()
    if (Option(filter.username).isDefined) {
      whereConditions += "username = ?"
      params += filter.username
    } else {
      whereConditions += s"username = '$DEFAULT_VALUE'"
    }
    if (Option(filter.requestName).isDefined) {
      whereConditions += "request_name = ?"
      params += filter.requestName
    } else {
      whereConditions += s"request_name = '$DEFAULT_VALUE'"
    }
    if (Option(filter.region).isDefined) {
      whereConditions += "region = ?"
      params += filter.region
    } else {
      whereConditions += s"region = '$DEFAULT_VALUE'"
    }
    if (Option(filter.engineType).isDefined) {
      whereConditions += "LOWER(engine_type) = ?"
      params += filter.engineType.toLowerCase(Locale.ROOT)
    } else {
      whereConditions += s"engine_type = '$DEFAULT_VALUE'"
    }
    if (Option(filter.cluster).isDefined) {
      whereConditions += "cluster = ?"
      params += filter.cluster
    } else {
      whereConditions += s"cluster = '$DEFAULT_VALUE'"
    }
    if (Option(filter.service).isDefined) {
      whereConditions += "service = ?"
      params += filter.service
    } else {
      whereConditions += s"service = '$DEFAULT_VALUE'"
    }
    if (whereConditions.nonEmpty) {
      queryBuilder.append(whereConditions.mkString(" WHERE ", " AND ", " "))
    }
    queryBuilder.append(" ORDER BY username ")
    val query = queryBuilder.toString()
    logger.debug(query)
    logger.debug(params.toString())
    (query, params)
  }

  private def buildBlacklist(resultSet: ResultSet): Seq[Blacklist] = {
    try {
      val blacklist = ListBuffer[Blacklist]()
      while (resultSet.next()) {
        val bl = Blacklist(
          username = resultSet.getString("username"),
          requestName = resultSet.getString("request_name"),
          region = resultSet.getString("region"),
          engineType = resultSet.getString("engine_type"),
          cluster = resultSet.getString("cluster"),
          service = resultSet.getString("service"),
          createTime = resultSet.getTimestamp("create_time"))
        blacklist += bl
      }
      blacklist
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
  }

  private def buildWhitelist(resultSet: ResultSet): Seq[Whitelist] = {
    try {
      val whitelist = ListBuffer[Whitelist]()
      while (resultSet.next()) {
        val wl = Whitelist(
          username = resultSet.getString("username"),
          requestName = resultSet.getString("request_name"),
          region = resultSet.getString("region"),
          engineType = resultSet.getString("engine_type"),
          cluster = resultSet.getString("cluster"),
          service = resultSet.getString("service"),
          createTime = resultSet.getTimestamp("create_time"),
          requestConf = string2Map(resultSet.getString("request_conf")),
          requestArgs = string2Seq(resultSet.getString("request_args")))
        whitelist += wl
      }
      whitelist
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
  }

  private def withResultSet[T](
      conn: Connection,
      sql: String,
      params: Any*)(f: ResultSet => T): T = {
    debug(s"executing sql $sql with result set")
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      resultSet = statement.executeQuery()
      f(resultSet)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (resultSet != null) {
        Utils.tryLogNonFatalError(resultSet.close())
      }
      if (statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
    }
  }

  private def setStatementParams(statement: PreparedStatement, params: Any*): Unit = {
    params.zipWithIndex.foreach { case (param, index) =>
      param match {
        case null => statement.setObject(index + 1, null)
        case s: String => statement.setString(index + 1, s)
        case i: Int => statement.setInt(index + 1, i)
        case l: Long => statement.setLong(index + 1, l)
        case d: Double => statement.setDouble(index + 1, d)
        case f: Float => statement.setFloat(index + 1, f)
        case b: Boolean => statement.setBoolean(index + 1, b)
        case _ => throw new KyuubiException(s"Unsupported param type ${param.getClass.getName}")
      }
    }
  }

  private def withConnection[T](autoCommit: Boolean = true)(f: Connection => T): T = {
    var connection: Connection = null
    try {
      connection = hikariDataSource.getConnection
      connection.setAutoCommit(autoCommit)
      f(connection)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (connection != null) {
        Utils.tryLogNonFatalError(connection.close())
      }
    }
  }

  private def valueAsString(obj: Any): String = {
    mapper.writeValueAsString(obj)
  }

  private def string2Map(str: String): Map[String, String] = {
    if (str == null || str.isEmpty) {
      Map.empty
    } else {
      mapper.readValue(str, classOf[Map[String, String]])
    }
  }

  private def string2Seq(str: String): Seq[String] = {
    if (str == null || str.isEmpty) {
      Seq.empty
    } else {
      mapper.readValue(str, classOf[Seq[String]])
    }
  }
}

object BWlistStore {
  val DEFAULT_VALUE = "all"
  private val BLACKLIST_TABLE = "blacklist"
  private val WHITELIST_TABLE = "whitelist"
  private val BLACKLIST_TABLE_COLUMNS = Seq(
    "username",
    "request_name",
    "engine_type",
    "cluster",
    "service",
    "create_time").mkString(",")
  private val WHITELIST_TABLE_COLUMNS = Seq(
    BLACKLIST_TABLE,
    "request_conf",
    "request_args").mkString(",")

  def main(args: Array[String]): Unit = {
    val conf = new KyuubiConf().loadFileDefaults()
    val bwl = new BWlistStore(conf)
    val bl = bwl.getBlacklist
    val wl = bwl.getWhitelist
    print(bl)
    print(wl)
  }
}
