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

package org.apache.kyuubi.util

import java.io.ByteArrayOutputStream
import java.util.{Base64, Locale}
import java.util.regex.Pattern
import java.util.zip.Inflater

import scala.collection.mutable

import com.alibaba.fastjson2.JSONObject
import io.lakecat.probe.model.SqlInfo
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.authorization.{AuthorizationManager, SqlAuth}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_DATABASE, KYUUBI_SESSION_CLUSTER_TAGS, KYUUBI_SESSION_GROUP_ID, KYUUBI_SESSION_TENANT}
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_REAL_USER_KEY, LAKECAT_CATALOG_PROBE_TASK_ID, SPARK_SQL_DEFAULT_DB_KEY}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder.SPARK_HADOOP_CONF_PREFIX

object SqlUtils extends Logging {

  def decodeSql(str: String): String = {
    val dataBytes = Base64.getDecoder.decode(str.getBytes())
    val decompressor = new Inflater()
    decompressor.reset()
    decompressor.setInput(dataBytes)
    val outputStream = new ByteArrayOutputStream(dataBytes.length)
    val buf = new Array[Byte](1024)
    while (!decompressor.finished()) {
      val i = decompressor.inflate(buf)
      outputStream.write(buf, 0, i)
    }
    outputStream.close()
    decompressor.end()
    outputStream.toString()
  }

  def parseSqlFromArgs(args: Seq[String]): String = {
    var srcSql: String = null
    var tmpSql: String = null
    if (null == args) {
      return srcSql
    }
    if (args.length == 1) {
      // json
      try {
        val json = JSONObject.parse(args.head)
        srcSql = json.getString("sql")
        tmpSql = srcSql
      } catch {
        case _: Exception =>
          debug("args not json")
      }
    } else {
      args.grouped(2).toList.collect {
        case Seq("-e", argSql: String) =>
          val i = argSql.length / 1024
          val count = argSql.trim.count(_ == ' ')
          debug(s"parseSql i: [$i], count: [$count]")
          srcSql = argSql
          tmpSql = if (count <= i + 1) {
            argSql.replaceAll(" ", "")
          } else {
            argSql
          }
      }
    }
    if (StringUtils.isBlank(tmpSql)) {
      return tmpSql
    } else {
      tmpSql = tmpSql.trim
    }
    debug(s"srcSql:[$srcSql], tmpSql:[$tmpSql]")
    if (isBase64(tmpSql)) {
      tmpSql = decodeSql(tmpSql)
      debug(s"decode sql: [$tmpSql]")
      return tmpSql
    }
    srcSql
  }

  def isBase64(str: String): Boolean = {
    val base64Pattern =
      "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
    Pattern.matches(base64Pattern, str)
  }

  def needAuth(enabled: Boolean, engineType: String, provider: String = ""): Boolean = {
    enabled &&
    (Seq("SPARK_SQL", "SPARK", "TRINO", "HIVE").contains(engineType) ||
      (engineType == "JDBC" && Seq("HiveConnectionProvider", "SparkConnectionProvider").contains(
        provider)))
  }

  def parseEngineType(engineType: String = "", provider: String = ""): SqlAuth.Engine =
    engineType.toUpperCase(Locale.ROOT) match {
      case "SPARK_SQL" | "SPARK" => SqlAuth.Engine.SPARK
      case "HIVE" => SqlAuth.Engine.HIVE
      case "TRINO" => SqlAuth.Engine.TRINO
      case "JDBC" =>
        if (provider == "HiveConnectionProvider") {
          SqlAuth.Engine.HIVE
        } else if (provider == "SparkConnectionProvider") {
          SqlAuth.Engine.SPARK
        } else {
          SqlAuth.Engine.UNKNOWN
        }
      case _ => SqlAuth.Engine.UNKNOWN
    }

  def packAuthError(user: String, sql: String, sqlInfo: SqlInfo): String = {
    info(
      s"User: [$user], SQL: [$sql], no permission!")
    val sb = new StringBuilder
    if (null != sqlInfo) {
      sqlInfo.getAuthorizationNowAllowList.forEach { a =>
        sb.append(
          s"object ${a.getCatalogInnerObject.getObjectName} no permission " +
            s"[${a.getOperation.getPrintName}]\n")
      }
    }
    sb.toString()
  }

  def authSql(
      authorizationManager: Option[AuthorizationManager],
      sql: String,
      engineName: SqlAuth.Engine,
      conf: KyuubiConf): SqlInfo = {
    authorizationManager match {
      case Some(am) =>
        val user = conf.getOption(KYUUBI_SESSION_REAL_USER_KEY).orNull
        val tenant = conf.get(KYUUBI_SESSION_TENANT)
        val clusterTags = conf.get(KyuubiConf.KYUUBI_SESSION_CLUSTER_TAGS)
        val region = TagsParser.getValueFromTags(clusterTags, "region")
        val provider = TagsParser.getValueFromTags(clusterTags, "provider")
        val catalog = am.genCatalog(tenant, provider, region)
        val database = conf.get(ENGINE_JDBC_CONNECTION_DATABASE)
        am.auth(
          sql,
          user,
          tenant,
          catalog,
          database,
          engineName)
      case _ => null
    }
  }

  def authSql(
      authorizationManager: Option[AuthorizationManager],
      sql: String,
      engineName: SqlAuth.Engine,
      conf: mutable.Map[String, String]): SqlInfo = {
    authorizationManager match {
      case Some(am) =>
        val user = conf.getOrElse(KYUUBI_SESSION_REAL_USER_KEY, "")
        val tenant = conf.getOrElse(KYUUBI_SESSION_TENANT.key, KYUUBI_SESSION_TENANT.defaultValStr)
        val clusterTags = conf.getOrElse(KyuubiConf.KYUUBI_SESSION_CLUSTER_TAGS.key, "")
        val region = TagsParser.getValueFromTags(clusterTags, "region")
        val provider = TagsParser.getValueFromTags(clusterTags, "provider")
        val catalog = am.genCatalog(tenant, provider, region)
        val database = conf.getOrElse(
          SPARK_SQL_DEFAULT_DB_KEY,
          conf.getOrElse(
            ENGINE_JDBC_CONNECTION_DATABASE.key,
            ENGINE_JDBC_CONNECTION_DATABASE.defaultValStr))
        am.auth(
          sql,
          user,
          tenant,
          catalog,
          database,
          engineName)
      case _ => null
    }
  }

  def pushSql(
      authorizationManager: Option[AuthorizationManager],
      sql: String,
      sqlInfo: SqlInfo,
      engineName: SqlAuth.Engine,
      conf: KyuubiConf): Unit = {
    authorizationManager match {
      case Some(am) =>
        val tenant = conf.get(KYUUBI_SESSION_TENANT)
        val clusterTags = conf.get(KYUUBI_SESSION_CLUSTER_TAGS)
        val region = TagsParser.getValueFromTags(clusterTags, "region")
        val provider = TagsParser.getValueFromTags(clusterTags, "provider")
        val catalog = am.genCatalog(tenant, provider, region)
        am.pushSqlInfo(
          sqlInfo,
          sql,
          conf.getOption(LAKECAT_CATALOG_PROBE_TASK_ID).getOrElse(""),
          conf.getOption(KYUUBI_SESSION_REAL_USER_KEY).getOrElse(""),
          conf.get(KYUUBI_SESSION_GROUP_ID),
          tenant,
          catalog,
          conf.get(ENGINE_JDBC_CONNECTION_DATABASE),
          engineName)
      case _ =>
    }
  }

  def pushSql(
      authorizationManager: Option[AuthorizationManager],
      sql: String,
      sqlInfo: SqlInfo,
      engineName: SqlAuth.Engine,
      conf: mutable.Map[String, String]): Unit = {
    authorizationManager match {
      case Some(am) =>
        val tenant = conf.getOrElse(KYUUBI_SESSION_TENANT.key, KYUUBI_SESSION_TENANT.defaultValStr)
        val clusterTags =
          conf.getOrElse(KYUUBI_SESSION_CLUSTER_TAGS.key, KYUUBI_SESSION_CLUSTER_TAGS.defaultValStr)
        val region = TagsParser.getValueFromTags(clusterTags, "region")
        val provider = TagsParser.getValueFromTags(clusterTags, "provider")
        val catalog = am.genCatalog(tenant, provider, region)
        am.pushSqlInfo(
          sqlInfo,
          sql,
          conf.getOrElse(SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_TASK_ID, ""),
          conf.getOrElse(KYUUBI_SESSION_REAL_USER_KEY, ""),
          conf.getOrElse(KYUUBI_SESSION_GROUP_ID.key, ""),
          tenant,
          catalog,
          conf.getOrElse(
            ENGINE_JDBC_CONNECTION_DATABASE.key,
            ENGINE_JDBC_CONNECTION_DATABASE.defaultValStr),
          engineName)
      case _ =>
    }
  }
}
