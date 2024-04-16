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

package org.apache.kyuubi.session

import java.util.Locale

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.authentication.{AuthenticationManager, Group}
import org.apache.kyuubi.authorization.AuthorizationManager
import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.cluster.{Cluster, ClusterManager}
import org.apache.kyuubi.cluster.config.LakecatConfig
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.credentials.HadoopCredentialsManager
import org.apache.kyuubi.engine.{EngineType, KyuubiApplicationManager}
import org.apache.kyuubi.engine.EngineType._
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{KyuubiOperationManager, OperationState}
import org.apache.kyuubi.plugin.{GroupProvider, PluginLoader, SessionConfAdvisor}
import org.apache.kyuubi.server.metadata.{MetadataManager, MetadataRequestsRetryRef}
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.server.metadata.jdbc.BWlistStore
import org.apache.kyuubi.session.trino.KyuubiTrinoSessionImpl
import org.apache.kyuubi.sql.parser.server.KyuubiParser
import org.apache.kyuubi.storage.StorageManager
import org.apache.kyuubi.util.SignUtils

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  protected[kyuubi] var clusterManager: Option[ClusterManager] = None

  protected[kyuubi] var authenticationManager: Option[AuthenticationManager] = None

  protected[kyuubi] var authorizationManager: Option[AuthorizationManager] = None

  protected[kyuubi] var storageManager: Option[StorageManager] = None

  private val parser = new KyuubiParser()

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()
  val applicationManager = new KyuubiApplicationManager()

  private lazy val metadataManager: Option[MetadataManager] = {
    // Currently, the metadata manager is used by the REST frontend which provides batch job APIs,
    // so we initialize it only when Kyuubi starts with the REST frontend.
    if (conf.get(FRONTEND_PROTOCOLS).map(FrontendProtocols.withName)
        .contains(FrontendProtocols.REST)) {
      Option(new MetadataManager())
    } else {
      None
    }
  }

  // lazy is required for plugins since the conf is null when this class initialization
  lazy val sessionConfAdvisor: SessionConfAdvisor = PluginLoader.loadSessionConfAdvisor(conf)
  lazy val groupProvider: GroupProvider = PluginLoader.loadGroupProvider(conf)

  private var limiter: Option[SessionLimiter] = None
  private var batchLimiter: Option[SessionLimiter] = None
  lazy val (signingPrivateKey, signingPublicKey) = SignUtils.generateKeyPair()

  lazy val bwlistStore = new BWlistStore(conf)

  lazy val clusterDynamic: Boolean = conf.get(KYUUBI_SESSION_SELECT_CLUSTER_DYNAMIC)

  def selectCluster(
      engineId: String,
      sessionType: String,
      clusterId: String,
      commandName: String,
      clusterTags: Option[String],
      commandTags: Option[String],
      selectorNames: Option[String],
      params: Map[String, String],
      dynamic: Boolean): Option[Cluster] = clusterManager match {
    case Some(cm) =>
      val cts = clusterTags.map { a => a.split(KYUUBI_CONF_SEPARATE_COMMA).toList }.getOrElse(Seq())
      val cmds =
        commandTags.map { a => a.split(KYUUBI_CONF_SEPARATE_COMMA).toList }.getOrElse(Seq())
      val names =
        selectorNames.getOrElse(
          KyuubiConf.KYUUBI_SESSION_CLUSTER_SELECTOR_NAMES.defaultValStr).split(
          KYUUBI_CONF_SEPARATE_COMMA).toList
      if (StringUtils.isEmpty(clusterId)) {
        selectCluster(
          engineId,
          sessionType,
          clusterId,
          commandName,
          cts,
          cmds,
          names,
          params,
          dynamic)
      } else {
        Option(cm.getCluster(sessionType, engineId, clusterId, commandName))
      }
    case _ => None
  }

  def selectCluster(
      engineId: String,
      engineType: String,
      clusterId: String,
      commandName: String,
      clusterTags: Seq[String],
      commandTags: Seq[String],
      selectorNames: Seq[String],
      params: Map[String, String],
      dynamic: Boolean = false): Option[Cluster] = clusterManager match {
    case Some(cm) =>
      val clu = cm.selectCluster(
        engineType,
        engineId,
        clusterTags.asJava,
        commandTags.asJava,
        selectorNames.asJava,
        params.asJava,
        dynamic)
      Option(clu)
    case _ => None
  }

  def getGroup(groupId: String, groupName: String): Option[Group] = authenticationManager match {
    case Some(am) =>
      val keytabPrefix = getConf.get(KYUUBI_SESSION_KEYTAB_PREFIX)
      val principalHost = getConf.get(KYUUBI_SESSION_PRINCIPAL_HOST)
      val principalRealm = getConf.get(KYUUBI_SESSION_PRINCIPAL_REALM)
      Option(am.buildAuthGroup(groupId, groupName, keytabPrefix, principalHost, principalRealm))
    case _ => None
  }

  def getClusterMaster(engineType: String, clusterId: String): String = clusterManager match {
    case Some(cm) =>
      val clusters = cm.getClusters(engineType)
      clusters.getClusters.forEach(a =>
        if (a.getId == clusterId) {
          return a.getMaster
        })
      Cluster.K8S_PREFIX
    case _ => null
  }

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    addService(applicationManager)
    addService(credentialsManager)
    metadataManager.foreach(addService)
    initSessionLimiter(conf)

    // init cluster manager
    var confPath = conf.get(KYUUBI_CLUSTER_CONFIG_PATH)
    if (confPath.isEmpty) {
      confPath = System.getenv().get(KyuubiConf.KYUUBI_HOME) + "/cluster_config"
    }
    try {
      clusterManager = Some(ClusterManager.load(confPath))
    } catch {
      case e: Exception =>
        logger.error("SessionManager clusterManager and authManager failed.", e)
        throw e
    }

    if (conf.get(KYUUBI_AUTHENTICATION_ENABLED)) {
      authenticationManager = Some(AuthenticationManager.load(confPath))
    }

    if (conf.get(KYUUBI_AUTHORIZATION_ENABLED)) {
      authorizationManager = Some(AuthorizationManager.load(
        new LakecatConfig(
          conf.get(KYUUBI_LAKECAT_CLIENT_HOST),
          conf.get(KYUUBI_LAKECAT_CLIENT_PORT),
          conf.get(KYUUBI_LAKECAT_CLIENT_TOKEN))))
    }

    storageManager = Some(new StorageManager(
      this,
      conf.get(STORAGE_EXEC_POOL_SIZE),
      conf.get(STORAGE_EXEC_POOL_MAX_SIZE),
      conf.get(STORAGE_EXEC_WAIT_QUEUE_SIZE),
      conf.get(STORAGE_EXEC_KEEPALIVE_TIME)))

    super.initialize(conf)
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    if ("thread" == this.conf.get(KYUUBI_SESSION_MODE)) {
      val engineType = conf.getOrElse(
        s"set:hiveconf:${KyuubiConf.ENGINE_TYPE.key}",
        conf.getOrElse(KyuubiConf.ENGINE_TYPE.key, ""))
        .toUpperCase(Locale.ROOT)
      EngineType.withName(engineType) match {
        case TRINO =>
          new KyuubiTrinoSessionImpl(
            protocol,
            user,
            password,
            ipAddress,
            conf,
            this,
            this.getConf.getUserDefaults(user),
            parser)
        case _ =>
          new KyuubiSessionImpl(
            protocol,
            user,
            password,
            ipAddress,
            conf,
            this,
            this.getConf.getUserDefaults(user),
            parser)
      }
    } else {
      new KyuubiSessionImpl(
        protocol,
        user,
        password,
        ipAddress,
        conf,
        this,
        this.getConf.getUserDefaults(user),
        parser)
    }
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    limiter.foreach(_.increment(UserIpAddress(username, ipAddress)))
    try {
      super.openSession(protocol, username, password, ipAddress, conf)
    } catch {
      case e: Throwable =>
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
          ms.incCount(MetricRegistry.name(CONN_FAIL, SessionType.INTERACTIVE.toString))
        }
        throw KyuubiSQLException(
          s"Error opening session for $username client ip $ipAddress, due to ${e.getMessage}",
          e)
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    info(s"CloseSession engine[${sessionHandle.toString}]")
    val session = getSession(sessionHandle)
    try {
      super.closeSession(sessionHandle)
    } finally {
      session match {
        case _: KyuubiBatchSessionImpl =>
          batchLimiter.foreach(_.decrement(UserIpAddress(session.user, session.ipAddress)))
        case _ =>
          limiter.foreach(_.decrement(UserIpAddress(session.user, session.ipAddress)))
      }
    }
  }

  private def createBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest,
      recoveryMetadata: Option[Metadata] = None): KyuubiBatchSessionImpl = {
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    new KyuubiBatchSessionImpl(
      username,
      password,
      ipAddress,
      conf,
      this,
      this.getConf.getUserDefaults(user),
      batchRequest,
      recoveryMetadata)
  }

  private[kyuubi] def openBatchSession(batchSession: KyuubiBatchSessionImpl): SessionHandle = {
    val user = batchSession.user
    val ipAddress = batchSession.ipAddress
    batchLimiter.foreach(_.increment(UserIpAddress(user, ipAddress)))
    val handle = batchSession.handle
    try {
      batchSession.open()
      setSession(handle, batchSession)
      logSessionCountInfo(batchSession, "opened")
      handle
    } catch {
      case e: Exception =>
        try {
          batchSession.close()
        } catch {
          case t: Throwable =>
            warn(s"Error closing batch session[$handle] for $user client ip: $ipAddress", t)
        }
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
          ms.incCount(MetricRegistry.name(CONN_FAIL, SessionType.BATCH.toString))
        }
        throw KyuubiSQLException(
          s"Error opening batch session[$handle] for $user client ip $ipAddress," +
            s" due to ${e.getMessage}",
          e)
    }
  }

  def openBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest): SessionHandle = {
    val batchSession = createBatchSession(user, password, ipAddress, conf, batchRequest)
    openBatchSession(batchSession)
  }

  def getBatchSessionImpl(sessionHandle: SessionHandle): KyuubiBatchSessionImpl = {
    getSessionOption(sessionHandle).map(_.asInstanceOf[KyuubiBatchSessionImpl]).orNull
  }

  def getInteractiveSessionImpl(sessionHandle: SessionHandle): KyuubiSessionImpl = {
    getSessionOption(sessionHandle).map(_.asInstanceOf[KyuubiSessionImpl]).orNull
  }

  def insertMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.insertMetadata(metadata))
  }

  def updateMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.updateMetadata(metadata))
  }

  def getMetadataRequestsRetryRef(identifier: String): Option[MetadataRequestsRetryRef] = {
    Option(metadataManager.map(_.getMetadataRequestsRetryRef(identifier)).orNull)
  }

  def deRegisterMetadataRequestsRetryRef(identifier: String): Unit = {
    metadataManager.foreach(_.deRegisterRequestsRetryRef(identifier))
  }

  def getBatchFromMetadataStore(batchId: String): Batch = {
    metadataManager.map(_.getBatch(batchId)).orNull
  }

  def getBatchesFromMetadataStore(
      name: String,
      batchType: String,
      batchUser: String,
      batchState: String,
      createTime: Long,
      endTime: Long,
      from: Int,
      size: Int): Seq[Batch] = {
    metadataManager.map(
      _.getBatches(name, batchType, batchUser, batchState, createTime, endTime, from, size))
      .getOrElse(Seq.empty)
  }

  def getBatchMetadata(batchId: String): Metadata = {
    metadataManager.map(_.getBatchSessionMetadata(batchId)).orNull
  }

  def getInteractiveMetadata(batchId: String): Metadata = {
    metadataManager.map(_.getInteractiveMetadata(batchId)).orNull
  }

  @VisibleForTesting
  def cleanupMetadata(identifier: String): Unit = {
    metadataManager.foreach(_.cleanupMetadataById(identifier))
  }

  override def start(): Unit = synchronized {
    MetricsSystem.tracing { ms =>
      ms.registerGauge(CONN_OPEN, getOpenSessionCount, 0)
      ms.registerGauge(EXEC_POOL_ALIVE, getExecPoolSize, 0)
      ms.registerGauge(EXEC_POOL_ACTIVE, getActiveCount, 0)
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    storageManager.foreach { sm =>
      sm.shutdown(conf.get(ENGINE_EXEC_POOL_SHUTDOWN_TIMEOUT))
    }
    super.stop()
  }

  def getBatchSessionsToRecover(kyuubiInstance: String): Seq[KyuubiBatchSessionImpl] = {
    val supportRecoverEngine = Seq("SPARK", "PYSPARK", "SCRIPT", "TFJOB")
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToRecover =>
      metadataManager.map(_.getBatchesRecoveryMetadata(
        stateToRecover.toString,
        kyuubiInstance,
        0,
        Int.MaxValue).filter { meta =>
        supportRecoverEngine.contains(meta.engineType.toUpperCase(Locale.ROOT))
      }.map { metadata =>
        val batchRequest = new BatchRequest(
          metadata.engineType,
          metadata.resource,
          metadata.className,
          metadata.requestName,
          metadata.requestConf.asJava,
          metadata.requestArgs.asJava)

        createBatchSession(
          metadata.username,
          "anonymous",
          metadata.ipAddress,
          metadata.requestConf,
          batchRequest,
          Some(metadata))
      }).getOrElse(Seq.empty)
    }
  }

  def getPeerInstanceClosedBatchSessions(kyuubiInstance: String): Seq[Metadata] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToKill =>
      metadataManager.map(_.getPeerInstanceClosedBatchesMetadata(
        stateToKill.toString,
        kyuubiInstance,
        0,
        Int.MaxValue)).getOrElse(Seq.empty)
    }
  }

  override protected def isServer: Boolean = true

  private def initSessionLimiter(conf: KyuubiConf): Unit = {
    val userLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_USER).getOrElse(0)
    val ipAddressLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_IPADDRESS).getOrElse(0)
    val userIpAddressLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
    val userUnlimitedList = conf.get(SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST)
    limiter = applySessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, userUnlimitedList)

    val userBatchLimit = conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER).getOrElse(0)
    val ipAddressBatchLimit = conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_IPADDRESS).getOrElse(0)
    val userIpAddressBatchLimit =
      conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
    batchLimiter = applySessionLimiter(
      userBatchLimit,
      ipAddressBatchLimit,
      userIpAddressBatchLimit,
      userUnlimitedList)
  }

  private[kyuubi] def getUnlimitedUsers(): Set[String] = {
    limiter.orElse(batchLimiter).map(SessionLimiter.getUnlimitedUsers).getOrElse(Set.empty)
  }

  private[kyuubi] def refreshUnlimitedUsers(conf: KyuubiConf): Unit = {
    val unlimitedUsers = conf.get(SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST).toSet
    limiter.foreach(SessionLimiter.resetUnlimitedUsers(_, unlimitedUsers))
    batchLimiter.foreach(SessionLimiter.resetUnlimitedUsers(_, unlimitedUsers))
  }

  private def applySessionLimiter(
      userLimit: Int,
      ipAddressLimit: Int,
      userIpAddressLimit: Int,
      userUnlimitedList: Seq[String]): Option[SessionLimiter] = {
    Seq(userLimit, ipAddressLimit, userIpAddressLimit).find(_ > 0).map(_ =>
      SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, userUnlimitedList.toSet))
  }
}
