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

package org.apache.kyuubi.session.trino

import java.io.File
import java.net.URI
import java.time.ZoneId
import java.util.{Collections, Locale, Optional}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import io.airlift.units.Duration
import io.trino.client.{ClientSession, OkHttpUtil}
import okhttp3.OkHttpClient
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.EngineType
import org.apache.kyuubi.events.{EventBus, KyuubiSessionEvent}
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionManager, SessionType}
import org.apache.kyuubi.session.SessionType.SessionType
import org.apache.kyuubi.sql.parser.server.KyuubiParser

class KyuubiTrinoSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    override val sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf,
    parser: KyuubiParser)
  extends KyuubiSession(protocol, user, password, ipAddress, conf, sessionManager, sessionConf) {

  private var clientSession: ClientSession = _
  private var httpClient: OkHttpClient = _
  private var catalogName: String = _
  private var databaseName: String = _

  override val sessionType: SessionType = SessionType.INTERACTIVE

  protected val sessionId: String = handle.identifier.toString

  override val normalizedConf: Map[String, String] =
    sessionManager.validateAndNormalizeConf(conf) ++ Map("spark.job.type" -> "SQL")

  val optimizedConf: Map[String, String] = {
    val confOverlay = sessionManager.sessionConfAdvisor.getConfOverlay(
      user,
      normalizedConf.asJava)
    if (confOverlay != null) {
      normalizedConf ++ confOverlay.asScala
    } else {
      warn(s"the server plugin return null value for user: $user, ignore it")
      normalizedConf
    }
  }

  // TODO: needs improve the hardcode
  optimizedConf.foreach {
    case ("use:catalog", _) =>
    case ("use:database", value) => sessionConf.set(ENGINE_JDBC_CONNECTION_DATABASE, value)
    case ("kyuubi.engine.pool.size.threshold", _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  private val sessionEvent = KyuubiSessionEvent(this)
  EventBus.post(sessionEvent)

  val cluster: Option[Cluster] = selectCluster(sessionConf)

  override def getSessionEvent: Option[KyuubiSessionEvent] = {
    Option(sessionEvent)
  }

  override def checkSessionAccessPathURIs(): Unit = {}

  override def open(): Unit = {
    traceMetricsOnOpen()

    authentication(sessionConf)

    checkSessionAccessPathURIs()

    insertMetadata(sessionConf, cluster)

    createClientSession()

    // we should call super.open before running launch engine operation
    super.open()
  }

  private def buildProperties(kyuubiConf: KyuubiConf): java.util.Map[String, String] = {
    val properties = new java.util.HashMap[String, String]()
    info(s"GetConnection sessionId[$sessionId] getProperties: " +
      s"[${kyuubiConf.get(ENGINE_JDBC_CONNECTION_PROPERTIES)}]")
    kyuubiConf.get(ENGINE_JDBC_CONNECTION_PROPERTIES).foreach { prop =>
      val tuple = prop.split("=", 2).map(_.trim)
      properties.put(tuple(0), tuple(1))
    }

    for (kv <- kyuubiConf.getAll) {
      if (kv._1.startsWith(ENGINE_JDBC_CONNECTION_PROPERTIES.key)) {
        val idx = kv._1.indexOf("=")
        val prop = kv._1.substring(idx + 1) + "=" + kv._2
        prop.split(",").foreach { a =>
          val tuple = a.split("=", 2).map(_.trim)
          properties.put(tuple(0), tuple(1))
        }
      }
    }
    info(s"GetConnection sessionId[$sessionId] buildProperties: [$properties]")
    properties
  }

  private def createClientSession(): Unit = {
    val builder = new OkHttpClient.Builder()
    if (sessionConf.get(KYUUBI_AUTHENTICATION_ENABLED)) {
      info(s"TrinoSession[$sessionId] authentication enabled:" +
        s"servicePrincipalPattern: [${sessionConf.get(KYUUBI_PRINCIPAL_PATTERN)}], " +
        s"remoteServiceName: [${sessionConf.get(ENGINE_TRINO_SERVER_NAME).orNull}], " +
        s"principal: [$user], " +
        s"kerberosConfig: [${sessionConf.get(KYUUBI_KRB5_CONF_PATH)}], " +
        s"keytab: [${sessionConf.get(SERVER_KEYTAB).orNull}], " +
        s"credentialCache: [${sessionConf.get(KYUUBI_KINIT_PATH)}]")
      OkHttpUtil.setupInsecureSsl(builder)
      OkHttpUtil.setupKerberos(
        builder,
        sessionConf.get(KYUUBI_PRINCIPAL_PATTERN),
        sessionConf.get(ENGINE_TRINO_SERVER_NAME).orNull,
        true,
        Optional.of(user),
        Optional.of(new File(sessionConf.get(KYUUBI_KRB5_CONF_PATH))),
        Optional.of(new File(sessionConf.get(SERVER_KEYTAB).orNull)),
        Optional.of(new File(sessionConf.get(KYUUBI_KINIT_PATH))))
    }

    httpClient = builder.build()

    val connectionUrl = cluster match {
      case Some(clu) =>
        clu.getMaster
      case _ =>
        sessionConf.get(KyuubiConf.ENGINE_TRINO_CONNECTION_URL).orNull
    }

    if (StringUtils.isEmpty(connectionUrl)) {
      throw KyuubiSQLException("Trino server url can not be null!")
    }

    catalogName = sessionConf.get(ENGINE_TRINO_CONNECTION_CATALOG).getOrElse(
      throw KyuubiSQLException("Trino default catalog can not be null!"))

    databaseName = sessionConf.get(ENGINE_JDBC_CONNECTION_DATABASE)

    val tenant = sessionConf.get(KYUUBI_SESSION_TENANT)

    val proxyUser = if (StringUtils.isNotEmpty(tenant)) {
      s"$tenant#$user"
    } else {
      user
    }

    clientSession = new ClientSession(
      URI.create(connectionUrl),
      if (sessionConf.get(KYUUBI_AUTHENTICATION_ENABLED)) user
      else "",
      Optional.of(proxyUser),
      "kyuubi",
      Optional.empty(),
      Collections.emptySet(),
      null,
      catalogName,
      databaseName,
      null,
      ZoneId.systemDefault(),
      Locale.getDefault,
      Collections.emptyMap(),
      buildProperties(sessionConf),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      null,
      new Duration(2, TimeUnit.MINUTES),
      true)
  }

  def createTrinoStatement(
      statement: String,
      operationLog: Option[OperationLog] = None): KyuubiTrinoStatement = {
    KyuubiTrinoStatement(httpClient, clientSession, sessionConf, statement, operationLog)
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def executeStatement(
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): OperationHandle = withAcquireRelease() {
    val incrementalCollect = normalizedConf.get(OPERATION_INCREMENTAL_COLLECT.key).map(
      _.toBoolean).getOrElse(sessionManager.getConf.get(OPERATION_INCREMENTAL_COLLECT))
    val operation = sessionManager.operationManager
      .newExecuteSyncStatementOperation(
        this,
        statement,
        confOverlay,
        runAsync,
        queryTimeout,
        EngineType.TRINO,
        incrementalCollect)
    runOperation(operation)
  }

  override def close(): Unit = {
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    super.close()
  }

}
