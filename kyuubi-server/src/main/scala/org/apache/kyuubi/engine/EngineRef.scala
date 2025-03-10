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

package org.apache.kyuubi.engine

import java.io.{FileWriter, IOException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.util.Random

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.commons.io.IOUtils

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.authentication.Group
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_SUBMIT_TIME_KEY
import org.apache.kyuubi.engine.EngineType.{EngineType, FLINK_SQL, HIVE_SQL, JDBC, SPARK_SQL, TRINO}
import org.apache.kyuubi.engine.ShareLevel.{CONNECTION, GROUP, SERVER, ShareLevel}
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder
import org.apache.kyuubi.engine.hive.HiveProcessBuilder
import org.apache.kyuubi.engine.jdbc.JdbcProcessBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.engine.trino.TrinoProcessBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryClientProvider, DiscoveryPaths}
import org.apache.kyuubi.metrics.MetricsConstants.{ENGINE_FAIL, ENGINE_TIMEOUT, ENGINE_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.BatchJobSubmission.{applicationCompleted, applicationPending, applicationRunning, PROC_LOGGER}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.KyuubiSessionImpl

/**
 * The description and functionality of an engine at server side
 *
 * @param conf Engine configuration
 * @param user Caller of the engine
 * @param engineRefId Id of the corresponding session in which the engine is created
 */
private[kyuubi] class EngineRef(
    conf: KyuubiConf,
    user: String,
    primaryGroup: String,
    engineRefId: String,
    engineManager: KyuubiApplicationManager,
    cluster: Option[Cluster] = None,
    session: KyuubiSessionImpl = null)
  extends Logging {
  // The corresponding ServerSpace where the engine belongs to
  private val serverSpace: String = conf.get(HA_NAMESPACE)

  private val timeout: Long = conf.get(ENGINE_INIT_TIMEOUT)

  private val applicationCheckInterval: Long =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_CHECK_INTERVAL)

  private var collectRemoteLogThread: Thread = _

  // Share level of the engine
  private val shareLevel: ShareLevel = ShareLevel.withName(conf.get(ENGINE_SHARE_LEVEL))

  private val engineType: EngineType = EngineType.withName(conf.get(ENGINE_TYPE))

  // Server-side engine pool size threshold
  private val poolThreshold: Int = conf.get(ENGINE_POOL_SIZE_THRESHOLD)

  private val clientPoolSize: Int = conf.get(ENGINE_POOL_SIZE)

  private val clientPoolName: String = conf.get(ENGINE_POOL_NAME)

  private val enginePoolIgnoreSubdomain: Boolean = conf.get(ENGINE_POOL_IGNORE_SUBDOMAIN)

  private val enginePoolSelectPolicy: String = conf.get(ENGINE_POOL_SELECT_POLICY)

  // In case the multi kyuubi instances have the small gap of timeout, here we add
  // a small amount of time for timeout
  private val LOCK_TIMEOUT_SPAN_FACTOR = if (Utils.isTesting) 0.5 else 0.1

  private var builder: ProcBuilder = _

  private[kyuubi] var group: Option[Group] = None

  // Launcher of the engine
  private[kyuubi] val appUser: String = shareLevel match {
    case SERVER => Utils.currentUser
    case GROUP => primaryGroup
    case _ => user
  }

  protected def getGroup: Option[Group] = {
    if (null == session) {
      return None
    }
    if (group.isEmpty) {
      session.sessionManager.getGroup(
        conf.get(KyuubiConf.KYUUBI_SESSION_GROUP_ID),
        conf.get(KyuubiConf.KYUUBI_SESSION_GROUP))
    }
    group
  }

  @VisibleForTesting
  private[kyuubi] val subdomain: String = conf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN) match {
    case subdomain if clientPoolSize > 0 && (subdomain.isEmpty || enginePoolIgnoreSubdomain) =>
      val poolSize = math.min(clientPoolSize, poolThreshold)
      if (poolSize < clientPoolSize) {
        warn(s"Request engine pool size($clientPoolSize) exceeds, fallback to " +
          s"system threshold $poolThreshold")
      }
      val seqNum = enginePoolSelectPolicy match {
        case "POLLING" =>
          val snPath =
            DiscoveryPaths.makePath(
              s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_${engineType}_seqNum",
              appUser,
              clientPoolName)
          DiscoveryClientProvider.withDiscoveryClient(conf) { client =>
            client.getAndIncrement(snPath)
          }
        case "RANDOM" =>
          Random.nextInt(poolSize)
      }
      s"$clientPoolName-${seqNum % poolSize}"
    case Some(_subdomain) => _subdomain
    case _ => "default" // [KYUUBI #1293]
  }

  /**
   * The default engine name, used as default `spark.app.name` if not set
   */
  @VisibleForTesting
  private[kyuubi] val defaultEngineName: String = {
    val commonNamePrefix = s"kyuubi_${shareLevel}_${engineType}_${appUser}"
    shareLevel match {
      case CONNECTION => s"${commonNamePrefix}_$engineRefId"
      case _ => s"${commonNamePrefix}_${subdomain}_$engineRefId"
    }
  }

  /**
   * The EngineSpace used to expose itself to the KyuubiServers in `serverSpace`
   *
   * For `CONNECTION` share level:
   *   /`serverSpace_version_CONNECTION_engineType`/`user`/`engineRefId`
   * For `USER` share level:
   *   /`serverSpace_version_USER_engineType`/`user`[/`subdomain`]
   * For `GROUP` share level:
   *   /`serverSpace_version_GROUP_engineType`/`primary group name`[/`subdomain`]
   * For `SERVER` share level:
   *   /`serverSpace_version_SERVER_engineType`/`kyuubi server user`[/`subdomain`]
   */
  @VisibleForTesting
  private[kyuubi] lazy val engineSpace: String = {
    val commonParent = s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_$engineType"
    shareLevel match {
      case CONNECTION => DiscoveryPaths.makePath(commonParent, appUser, engineRefId)
      case _ => DiscoveryPaths.makePath(commonParent, appUser, subdomain)
    }
  }

  /**
   * The distributed lock path used to ensure only once engine being created for non-CONNECTION
   * share level.
   */
  private def tryWithLock[T](discoveryClient: DiscoveryClient)(f: => T): T =
    shareLevel match {
      case CONNECTION => f
      case _ =>
        val lockPath =
          DiscoveryPaths.makePath(
            s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_${engineType}_lock",
            appUser,
            subdomain)
        discoveryClient.tryWithLock(
          lockPath,
          timeout + (LOCK_TIMEOUT_SPAN_FACTOR * timeout).toLong)(f)
    }

  private def create(
      discoveryClient: DiscoveryClient,
      extraEngineLog: Option[OperationLog]): (String, Int) = tryWithLock(discoveryClient) {
    // Get the engine address ahead if another process has succeeded
    var engineRef = discoveryClient.getServerHost(engineSpace)
    if (engineRef.nonEmpty) return engineRef.get

    conf.set(HA_NAMESPACE, engineSpace)
    conf.set(HA_ENGINE_REF_ID, engineRefId)
    val started = System.currentTimeMillis()
    conf.set(KYUUBI_ENGINE_SUBMIT_TIME_KEY, String.valueOf(started))

    var enableCollectRemoteLog = false
    builder = engineType match {
      case SPARK_SQL =>
        conf.setIfMissing(SparkProcessBuilder.APP_KEY, defaultEngineName)
        enableCollectRemoteLog = true
        new SparkProcessBuilder(
          appUser,
          conf,
          engineRefId,
          extraEngineLog,
          cluster)
          .resetSparkHome()
          .setProcEnv()
          .setProcAuthEnv(cluster.orNull)
      case FLINK_SQL =>
        conf.setIfMissing(FlinkProcessBuilder.APP_KEY, defaultEngineName)
        enableCollectRemoteLog = true
        new FlinkProcessBuilder(appUser, conf, engineRefId, extraEngineLog)
      case TRINO =>
        new TrinoProcessBuilder(
          appUser,
          conf,
          engineRefId,
          extraEngineLog,
          cluster)
          .setProcAuthEnv(cluster.orNull)
      case HIVE_SQL =>
        new HiveProcessBuilder(appUser, conf, engineRefId, extraEngineLog)
      case JDBC =>
        new JdbcProcessBuilder(
          appUser,
          conf,
          engineRefId,
          extraEngineLog,
          cluster)
          .setProcAuthEnv(cluster.orNull)
    }

    MetricsSystem.tracing(_.incCount(ENGINE_TOTAL))
    try {
      val redactedCmd = builder.toString
      info(s"Launching engine:\n$redactedCmd")
      debug(s"Launching engine env:\n${builder.getProcEnvs}")
      builder.validateConf()
      val process = builder.start
      var exitValue: Option[Int] = None
      while (engineRef.isEmpty) {
        if (exitValue.isEmpty && process.waitFor(1, TimeUnit.SECONDS)) {
          exitValue = Some(process.exitValue())
          if (exitValue.get != 0) {
            val error = builder.getError
            MetricsSystem.tracing { ms =>
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, appUser))
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, error.getClass.getSimpleName))
            }
            throw error
          }
        }

        // even the submit process succeeds, the application might meet failure when initializing,
        // check the engine application state from engine manager and fast fail on engine terminate
        if (exitValue.contains(0)) {
          Option(engineManager).foreach { engineMgr =>
            engineMgr.getApplicationInfo(
              builder.clusterManager(),
              engineRefId,
              cluster,
              getGroup).foreach { appInfo =>
              if (ApplicationState.isTerminated(appInfo.state)) {
                MetricsSystem.tracing { ms =>
                  ms.incCount(MetricRegistry.name(ENGINE_FAIL, appUser))
                  ms.incCount(MetricRegistry.name(ENGINE_FAIL, "ENGINE_TERMINATE"))
                }
                throw new KyuubiSQLException(
                  s"""
                     |The engine application has been terminated. Please check the engine log.
                     |ApplicationInfo: ${appInfo.toMap.mkString("(\n", ",\n", "\n)")}
                     |""".stripMargin,
                  builder.getError)
              }
            }
          }
        }

        if (started + timeout <= System.currentTimeMillis()) {
          val killMessage =
            engineManager.killApplication(
              builder.clusterManager(),
              engineRefId,
              cluster,
              getGroup)
          process.destroyForcibly()
          MetricsSystem.tracing(_.incCount(MetricRegistry.name(ENGINE_TIMEOUT, appUser)))
          throw KyuubiSQLException(
            s"Timeout($timeout ms, you can modify ${ENGINE_INIT_TIMEOUT.key} to change it) to" +
              s" launched $engineType engine with $redactedCmd. $killMessage",
            builder.getError)
        }
        engineRef = discoveryClient.getEngineByRefId(engineSpace, engineRefId)
        if (engineRef.isEmpty) {
          Thread.sleep(applicationCheckInterval)
        }
      }
      if (enableCollectRemoteLog) {
        collectRemoteLog()
      }
      engineRef.get
    } finally {
      // we must close the process builder whether session open is success or failure since
      // we have a log capture thread in process builder.
      builder.close()
      builder.clean()
    }
  }

  def currentApplicationInfo: Option[ApplicationInfo] = Option(engineManager) match {
    case Some(em) =>
      em.getApplicationInfo(builder.clusterManager(), engineRefId, cluster, getGroup)
    case _ => None
  }

  private def collectRemoteLog(): Unit = Option(engineManager) match {
    case Some(em) =>
      info(s"CollectRemoteLog $engineType engine[$engineRefId]")
      val runner: Runnable = () => {
        try {
          var app = currentApplicationInfo
          if (app.isEmpty) {
            return
          }

          while (!applicationRunning(app) || applicationPending(app)) {
            info(s"CollectRemoteLog engine[$engineRefId] appState: [${app.map(_.state).orNull}] " +
              s"wait: [${applicationCheckInterval}ms]")
            Thread.sleep(applicationCheckInterval)

            app = currentApplicationInfo
            if (app.isEmpty) {
              return
            }
          }
          info(s"CollectRemoteLog engine[$engineRefId] start log:")
          if (EngineType.SPARK_SQL == engineType) {
            info(s"Application status for ${app.get.id} (phase: Running) Spark context " +
              s"Web UI available at ${app.get.url.orNull}")
          }
          while (true) {
            em.getApplicationLog(
              builder.clusterManager(),
              engineRefId,
              cluster,
              getGroup) match {
              case Some(input) =>
                info(s"CollectRemoteLog engine[$engineRefId] got input")
                var writer: FileWriter = null;
                try {
                  writer = new FileWriter(builder.remoteLog)
                  IOUtils.copy(input, writer, StandardCharsets.UTF_8)
                } catch {
                  case e: IOException =>
                    error(s"CollectRemoteLog engine[$engineRefId] get log failed:", e)
                  case _: InterruptedException =>
                    warn(s"CollectRemoteLog engine[$engineRefId] get log interrupted at inner")
                } finally {
                  if (null != writer) {
                    writer.flush()
                    writer.close()
                  }
                  if (null != input) {
                    input.close()
                  }
                }
                info(s"CollectRemoteLog engine[$engineRefId] get log finished")
                return
              case None =>
                if (applicationCompleted(app)) {
                  error(s"CollectRemoteLog engine[$engineRefId] get log None," +
                    s" appState: [${app.map(_.state).orNull}] terminated")

                  return
                } else {
                  error(s"CollectRemoteLog engine[$engineRefId] get log None," +
                    s" appState: [${app.map(_.state).orNull}] not terminated, " +
                    s"wait [${applicationCheckInterval}ms]")
                  Thread.sleep(applicationCheckInterval)
                }
            }
          }
        } catch {
          case _: InterruptedException =>
            warn(s"CollectRemoteLog engine[$engineRefId] get log interrupted at outer")
            return
        } finally {
          info(s"Close engine[$engineRefId] mergeLogAndUpload")
          val opLog = OperationLog.findOperationLog(
            session.sessionManager.operationLogRoot.orNull,
            engineRefId)
          ProcBuilder.mergeLogAndUpload(
            engineRefId,
            session.createTime,
            opLog,
            builder.mergeLog.getAbsolutePath,
            conf)
        }
      }
      collectRemoteLogThread = PROC_LOGGER.newThread(runner)
      collectRemoteLogThread.start()
    case _ =>
  }

  /**
   * Get the engine ref from engine space first or create a new one
   *
   * @param discoveryClient the zookeeper client to get or create engine instance
   * @param extraEngineLog the launch engine operation log, used to inject engine log into it
   */
  def getOrCreate(
      discoveryClient: DiscoveryClient,
      extraEngineLog: Option[OperationLog] = None): (String, Int) = {
    discoveryClient.getServerHost(engineSpace)
      .getOrElse {
        create(discoveryClient, extraEngineLog)
      }
  }

  def close(): Unit = {
    if (shareLevel == CONNECTION && builder != null) {
      try {
        info(s"Close engine[$engineRefId]")
        builder.close(true)
        engineManager.killApplication(
          builder.clusterManager(),
          engineRefId,
          cluster,
          getGroup)
      } catch {
        case e: Exception =>
          warn(s"Error closing engine builder, engineRefId: $engineRefId", e)
      }
    }
  }
}
