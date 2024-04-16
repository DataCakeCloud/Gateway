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

import com.codahale.metrics.MetricRegistry
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_CONNECTION_URL_KEY, KYUUBI_SESSION_REAL_USER_KEY}
import org.apache.kyuubi.engine.ApplicationState
import org.apache.kyuubi.events.KyuubiSessionEvent
import org.apache.kyuubi.metrics.MetricsConstants.{CONN_OPEN, CONN_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.SessionType.SessionType
import org.apache.kyuubi.util.AuthUtil

abstract class KyuubiSession(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    val sessionConf: KyuubiConf)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  val sessionType: SessionType

  val connectionUrl: String = conf.getOrElse(KYUUBI_SESSION_CONNECTION_URL_KEY, "")

  val realUser: String = conf.getOrElse(KYUUBI_SESSION_REAL_USER_KEY, user)

  def getSessionEvent: Option[KyuubiSessionEvent]

  def checkSessionAccessPathURIs(): Unit

  private[kyuubi] def handleSessionException(f: => Unit): Unit = {
    try {
      f
    } catch {
      case t: Throwable =>
        getSessionEvent.foreach(_.exception = Some(t))
        throw t
    }
  }

  protected def authentication(sessionConf: KyuubiConf): Unit = {
    AuthUtil.authentication(sessionConf, sessionManager.authenticationManager)
  }

  protected def traceMetricsOnOpen(): Unit = MetricsSystem.tracing { ms =>
    ms.incCount(CONN_TOTAL)
    ms.incCount(MetricRegistry.name(CONN_TOTAL, sessionType.toString))
    ms.incCount(MetricRegistry.name(CONN_OPEN, user))
    ms.incCount(MetricRegistry.name(CONN_OPEN, sessionType.toString))
  }

  protected def traceMetricsOnClose(): Unit = MetricsSystem.tracing { ms =>
    ms.decCount(MetricRegistry.name(CONN_OPEN, user))
    ms.decCount(MetricRegistry.name(CONN_OPEN, sessionType.toString))
  }

  protected def selectCluster(
      sessionConf: KyuubiConf,
      clusterId: String = null,
      commandName: String = null,
      dynamic: Boolean = false): Option[Cluster] = {
    sessionManager.selectCluster(
      handle.identifier.toString,
      sessionConf.get(ENGINE_TYPE),
      clusterId,
      commandName,
      Option(sessionConf.get(KYUUBI_SESSION_CLUSTER_TAGS).mkString(",")),
      Option(sessionConf.get(KYUUBI_SESSION_COMMAND_TAGS).mkString(",")),
      Option(sessionConf.get(KYUUBI_SESSION_CLUSTER_SELECTOR_NAMES).mkString(",")),
      buildClusterCustomParams(),
      dynamic)
  }

  protected def buildClusterCustomParams(): Map[String, String] = {
    Map(
      "spark.kubernetes.executor.node.selector.lifecycle" -> normalizedConf.getOrElse(
        "spark.kubernetes.executor.node.selector.lifecycle",
        ""),
      "spark.executor.memory" -> normalizedConf.getOrElse("spark.executor.memory", ""),
      "spark.executor.cores" -> normalizedConf.getOrElse("spark.executor.cores", ""),
      "spark.kubernetes.executor.request.cores" -> normalizedConf.getOrElse(
        "spark.kubernetes.executor.request.cores",
        ""),
      "spark.executor.memoryOverhead" -> normalizedConf.getOrElse(
        "spark.executor.memoryOverhead",
        ""),
      "spark.executorEnv.spark.kubernetes.driver.reusePersistentVolumeClaim" ->
        normalizedConf.getOrElse(
          "spark.executorEnv.spark.kubernetes.driver.reusePersistentVolumeClaim",
          ""),
      "spark.kubernetes.container.image" -> normalizedConf.getOrElse(
        "spark.kubernetes.container.image",
        ""),
      "jobType" -> normalizedConf.getOrElse("spark.job.type", "Other"))
  }

  protected def insertMetadata(sessionConf: KyuubiConf, cluster: Option[Cluster]): Unit = {
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
  }
}
