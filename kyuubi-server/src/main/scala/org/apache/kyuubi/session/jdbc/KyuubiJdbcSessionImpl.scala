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

package org.apache.kyuubi.session.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.hive.service.rpc.thrift.{TGetResultSetMetadataResp, TProtocolVersion, TRowSet}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{ApplicationState, EngineType, KyuubiApplicationManager}
import org.apache.kyuubi.events.KyuubiSessionEvent
import org.apache.kyuubi.operation.{OperationHandle, OperationState, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, SessionType}
import org.apache.kyuubi.session.SessionType.SessionType
import org.apache.kyuubi.sql.parser.server.KyuubiParser
import org.apache.kyuubi.util.AuthUtil

class KyuubiJdbcSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf,
    parser: KyuubiParser)
  extends KyuubiSessionImpl(
    protocol,
    user,
    password,
    ipAddress,
    conf,
    sessionManager,
    sessionConf,
    parser) {

  private[kyuubi] var _connection: Connection = _
  def connection: Connection = _connection

  private val sessionEvent = KyuubiSessionEvent(this)

  override val sessionType: SessionType = SessionType.INTERACTIVE

  override def getSessionEvent: Option[KyuubiSessionEvent] = {
    Option(sessionEvent)
  }

  override def checkSessionAccessPathURIs(): Unit = {
    KyuubiApplicationManager.checkApplicationAccessPaths(
      sessionConf.get(ENGINE_TYPE),
      sessionConf.getAll,
      sessionManager.getConf)
  }

  override def open(): Unit = handleSessionException {
    traceMetricsOnOpen()

    authentication(sessionConf)

    checkSessionAccessPathURIs()

    // add clusterName and commandName into metadata by smy
    val clusterId = cluster.map(clu => clu.getId)
    val command = cluster.map(clu => {
      val cmd = clu.getCommand
      if (null != cmd) {
        cmd.getName
      } else {
        null
      }
    })
    var confMap = normalizedConf
    command.foreach { a =>
      confMap += (KyuubiConf.KYUUBI_BATCH_COMMAND_NAME.key -> a)
    }
    val metaData = Metadata(
      identifier = handle.identifier.toString,
      sessionType = sessionType,
      realUser = realUser,
      username = user,
      ipAddress = ipAddress,
      kyuubiInstance = connectionUrl,
      state = OperationState.FINISHED.toString,
      resource = "",
      className = "",
      requestName = name.orNull,
      requestConf = confMap,
      requestArgs = Seq.empty,
      createTime = createTime,
      engineType = sessionConf.get(ENGINE_TYPE),
      engineState = ApplicationState.FINISHED.toString,
      clusterManager = clusterId)

    // there is a chance that operation failed w/ duplicated key error
    sessionManager.insertMetadata(metaData)
    info(s"insert metadata successfully ${handle.identifier.toString} $clusterId")

    OperationLog.createOperationLogRootDirectory(this)

    openConnection()
  }

  private def buildProperties(kyuubiConf: KyuubiConf): Properties = {
    val properties = new Properties()

    val user = kyuubiConf.get(ENGINE_JDBC_CONNECTION_USER)
    if (user.isDefined) {
      properties.setProperty("user", user.get)
    }

    val password = kyuubiConf.get(ENGINE_JDBC_CONNECTION_PASSWORD)
    if (password.isDefined) {
      properties.setProperty("password", password.get)
    }

    info(s"GetConnection properties: [${kyuubiConf.get(ENGINE_JDBC_CONNECTION_PROPERTIES)}]")
    kyuubiConf.get(ENGINE_JDBC_CONNECTION_PROPERTIES).foreach { prop =>
      val tuple = prop.split("=", 2).map(_.trim)
      properties.setProperty(tuple(0), tuple(1))
    }
    properties
  }

  def openConnection(): Unit = {
    cluster.foreach { clu =>
      val database = sessionConf.get(ENGINE_JDBC_CONNECTION_DATABASE)
      var url = clu.getMaster
      if (url.endsWith("/")) {
        url += database
      } else {
        url += ("/" + database)
      }

      if (null != clu.getConf) {
        clu.getConf.forEach { kv =>
          url += s";$kv"
        }
      }

      // deal with authentication and user
      sessionConf.get(AUTHENTICATION_METHOD).head match {
        case AuthUtil.AUTHENTICATION_METHOD_KERBEROS =>
          val group = sessionConf.get(KYUUBI_SESSION_GROUP)
          val principal = sessionConf.get(KYUUBI_HIVE_SERVER_PRINCIPAL)
          url += s";auth=${AuthUtil.AUTHENTICATION_METHOD_KERBEROS};principal=$principal;"
          sessionConf.set(ENGINE_JDBC_CONNECTION_USER, s"$group")
        case _ =>
          val provider = sessionConf.get(ENGINE_JDBC_CONNECTION_PROVIDER).orNull
          if (Seq("SparkConnectionProvider", "HiveConnectionProvider").contains(provider)) {
            val tenant = sessionConf.get(KYUUBI_SESSION_TENANT)
            sessionConf.set(ENGINE_JDBC_CONNECTION_USER, s"$tenant#$user")
          }
      }

      sessionConf.set(ENGINE_JDBC_CONNECTION_URL, url)
    }

    val url = sessionConf.get(ENGINE_JDBC_CONNECTION_URL).orNull
    val prop = buildProperties(sessionConf)
    _connection = DriverManager.getConnection(url, prop)
  }

  override def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = withAcquireRelease() {
    val operation = sessionManager.operationManager
      .newExecuteSyncStatementOperation(
        this,
        statement,
        confOverlay,
        runAsync,
        queryTimeout,
        EngineType.JDBC)
    runOperation(operation)
  }

  def getOperationStatus(
      operationHandle: OperationHandle,
      maxWait: Option[Long] = None): OperationStatus = {
    null
  }

  override def getQueryId(operationHandle: OperationHandle): String = {
    null
  }

  override def getResultSetMetadata(operationHandle: OperationHandle): TGetResultSetMetadataResp = {
    null
  }

  override def fetchResults(
      operationHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Int,
      fetchLog: Boolean): TRowSet = {
    null
  }

  override def closeOperation(operationHandle: OperationHandle): Unit = {}

  override def close(): Unit = {
    if (null != _connection) {
      _connection.close()
      _connection = null
    }
  }
}
