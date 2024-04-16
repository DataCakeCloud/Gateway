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

package org.apache.kyuubi.operation

import java.io.{FileWriter, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Locale

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.commons.io.IOUtils
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.{KyuubiException, KyuubiSQLException}
import org.apache.kyuubi.authentication.Group
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine._
import org.apache.kyuubi.engine.spark.SparkBatchProcessBuilder
import org.apache.kyuubi.metrics.MetricsConstants.OPERATION_OPEN
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.{isTerminal, CANCELED, OperationState, RUNNING}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiBatchSessionImpl
import org.apache.kyuubi.util.NamedThreadFactory

/**
 * The state of batch operation is special. In general, the lifecycle of state is:
 *
 *                        /  ERROR
 * PENDING  ->  RUNNING  ->  FINISHED
 *                        \  CANCELED (CLOSED)
 *
 * We can not change FINISHED/ERROR/CANCELED to CLOSED, and it's different with other operation
 * which final status is always CLOSED, so we do not use CLOSED state in this class.
 * To compatible with kill application we combine the semantics of `cancel` and `close`, so if
 * user close the batch session that means the final status is CANCELED.
 */
class BatchJobSubmission(
    session: KyuubiBatchSessionImpl,
    val batchType: String,
    val batchName: String,
    resource: String,
    className: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    recoveryMetadata: Option[Metadata],
    val cluster: Option[Cluster] = None)
  extends KyuubiApplicationOperation(session) {
  import BatchJobSubmission._

  override def shouldRunAsync: Boolean = true

  protected val _operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)

  protected val applicationManager: KyuubiApplicationManager =
    session.sessionManager.applicationManager

  private[kyuubi] val batchId: String = session.handle.identifier.toString
  private[kyuubi] var group: Option[Group] = None

  protected var collectRemoteLogThread: Thread = _
  protected val collectRemoteLogLocker: Object = new Object

  @volatile protected var _applicationInfo: Option[ApplicationInfo] = None
  def getOrFetchCurrentApplicationInfo: Option[ApplicationInfo] = _applicationInfo match {
    case Some(_) => _applicationInfo
    case None => currentApplicationInfo
  }

  protected var killMessage: KillResponse = (false, "UNKNOWN")
  def getKillMessage: KillResponse = killMessage

  @volatile protected var _appStartTime: Long = recoveryMetadata.map(_.engineOpenTime).getOrElse(0L)
  def appStartTime: Long = _appStartTime

  @VisibleForTesting
  protected[kyuubi] lazy val builder: ProcBuilder = {
    Option(batchType).map(_.toUpperCase(Locale.ROOT)) match {
      case Some("SPARK") | Some("PYSPARK") =>
        new SparkBatchProcessBuilder(
          session.user,
          session.sessionConf,
          batchId,
          batchName,
          Option(resource),
          className,
          batchConf,
          batchArgs,
          getOperationLog,
          cluster)
          .resetSparkHome()
          .setProcEnv()
      case _ =>
        throw new UnsupportedOperationException(s"Batch type $batchType unsupported")
    }
  }

  protected def getGroup: Option[Group] = {
    if (group.isEmpty) {
      group = session.sessionManager.getGroup(
        batchConf.getOrElse(KyuubiConf.KYUUBI_SESSION_GROUP_ID.key, ""),
        batchConf.getOrElse(KyuubiConf.KYUUBI_SESSION_GROUP.key, ""))
    }
    group
  }

  override protected def currentApplicationInfo: Option[ApplicationInfo] = {
    debug(s"currentApplicationInfo group:[$getGroup]")
    var applicationInfo = _applicationInfo
    debug(s"CurrentApplicationInfo batch[$batchId] curApp: $applicationInfo")
    if (applicationCompleted(applicationInfo) && applicationInfo.nonEmpty) return applicationInfo
    // only the ApplicationInfo with non-empty id is valid for the operation
    applicationInfo =
      applicationManager.getApplicationInfo(
        builder.clusterManager(),
        batchId,
        cluster,
        getGroup).filter(_.id != null)
    debug(s"CurrentApplicationInfo batch[$batchId] newApp: $applicationInfo")
    applicationInfo.foreach { _ =>
      if (_appStartTime <= 0) {
        _appStartTime = System.currentTimeMillis()
      }
    }
    applicationInfo
  }

  protected def killBatchApplication(): KillResponse = {
    // interrupt log thread by smy
    val app = _applicationInfo
    info(s"KillBatchApplication batch[$batchId] curApp: $app")
    collectRemoteLogLocker.synchronized {
      info(s"KillBatchApplication batch[$batchId] notify thread and " +
        s"wait: [${applicationCheckInterval}ms]")
      collectRemoteLogLocker.notify()
    }
    Thread.sleep(applicationCheckInterval)
    if (null != collectRemoteLogThread) {
      collectRemoteLogThread.interrupt()
      collectRemoteLogThread = null
    }

    val (killed, msg) = applicationManager.killApplication(
      builder.clusterManager(),
      batchId,
      cluster,
      getGroup)
    try {
      builder.clean()
    } catch {
      case e: Exception =>
        error("KillBatchApplication batch[$batchId] builder clean failed", e)
    }
    /*
    OperationLog.getCurrentOperationLog match {
      case Some(opLog) =>
        ProcBuilder.mergeLogAndUpload(
          batchId,
          session.createTime,
          opLog,
          builder.mergeLog.getAbsolutePath,
          session.sessionConf)
      case _ =>
    }
     */
    (killed, msg)
  }

  protected val applicationCheckInterval: Long =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_CHECK_INTERVAL)
  protected val applicationStarvationTimeout: Long =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_STARVATION_TIMEOUT)
  protected val applicationPendingTimeout: Long =
    session.sessionConf.get(KyuubiConf.BATCH_APPLICATION_PENDING_TIMEOUT)

  protected def updateBatchMetadata(): Unit = {
    val endTime =
      if (isTerminalState(state)) {
        lastAccessTime
      } else {
        0L
      }

    if (isTerminalState(state)) {
      if (_applicationInfo.isEmpty) {
        _applicationInfo =
          Option(ApplicationInfo(id = null, name = null, state = ApplicationState.NOT_FOUND))
      }
    }

    _applicationInfo.foreach { status =>
      val metadataToUpdate = Metadata(
        identifier = batchId,
        state = state.toString,
        engineOpenTime = appStartTime,
        engineId = status.id,
        engineName = status.name,
        engineUrl = status.url.orNull,
        engineState = status.state.toString,
        engineError = status.error,
        endTime = endTime)
      session.sessionManager.updateMetadata(metadataToUpdate)
    }
  }

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  // we can not set to other state if it is canceled
  protected def setStateIfNotCanceled(newState: OperationState): Unit = state.synchronized {
    if (state != CANCELED) {
      setState(newState)
      _applicationInfo.filter(_.id != null).foreach { ai =>
        session.getSessionEvent.foreach(_.engineId = ai.id)
      }
      if (newState == RUNNING) {
        session.onEngineOpened()
      }
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setStateIfNotCanceled(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = session.handleSessionException {
    val asyncOperation: Runnable = () => {
      try {
        if (recoveryMetadata.exists(_.peerInstanceClosed)) {
          setState(OperationState.CANCELED)
        } else {
          // If it is in recovery mode, only re-submit batch job if previous state is PENDING and
          // fail to fetch the status including appId from resource manager. Otherwise, monitor the
          // submitted batch application.
          recoveryMetadata.map { metadata =>
            if (metadata.state == OperationState.PENDING.toString) {
              _applicationInfo = currentApplicationInfo
              val applicationInfo = _applicationInfo
              info(s"RunInternal batch[$batchId] curApp: $applicationInfo")
              applicationInfo.map(_.id) match {
                case Some(null) =>
                  submitAndMonitorBatchJob()
                case Some(appId) =>
                  monitorBatchJob(appId)
                  collectRemoteLog()
                case None =>
                  submitAndMonitorBatchJob()
              }
            } else {
              monitorBatchJob(metadata.engineId)
              collectRemoteLog()
            }
          }.getOrElse {
            submitAndMonitorBatchJob()
          }
          setStateIfNotCanceled(OperationState.FINISHED)
        }
      } catch {
        onError()
      } finally {
        updateBatchMetadata()
        val resp = killBatchApplication()
        info(s"Batch[$batchId] finished and kill app resp: $resp")
      }
    }

    try {
      val opHandle = session.sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch {
      onError("submitting batch job submission operation in background, request rejected")
    } finally {
      if (isTerminalState(state)) {
        updateBatchMetadata()
      }
    }
  }

  protected def startTimeout(currentTime: Long): Boolean = {
    currentTime - createTime > 3 * applicationStarvationTimeout
  }

  protected def pendingTimeout(currentTime: Long): Boolean = {
    currentTime - createTime > applicationPendingTimeout
  }

  private def submitAndMonitorBatchJob(): Unit = {
    var appStatusFirstUpdated = false
    var lastStarvationCheckTime = createTime
    try {
      info(s"Submitting $batchType batch[$batchId] job:\n$builder")
      debug(s"Launching engine env:\n${builder.getProcEnvs}")
      val process = builder.start
      _applicationInfo = currentApplicationInfo
      var app = _applicationInfo
      info(s"Submitting batch[$batchId] curApp: $app")
      // while (!applicationFailed(_applicationInfo) && process.isAlive) {
      while (!applicationRunning(_applicationInfo) && !isTerminal(state)) {
        val currentTime = System.currentTimeMillis()
        if (!appStatusFirstUpdated) {
          if (_applicationInfo.isDefined) {
            setStateIfNotCanceled(OperationState.RUNNING)
            updateBatchMetadata()
            appStatusFirstUpdated = true
          } else {
            if (currentTime - lastStarvationCheckTime > applicationStarvationTimeout) {
              lastStarvationCheckTime = currentTime
              warn(s"Batch[$batchId] has not started, check the Kyuubi server to ensure" +
                s" that batch jobs can be submitted.")
              if (startTimeout(currentTime)) {
                error(s"Batch[$batchId] has not started and timeout so terminated inner")
                throw new KyuubiException("Batch job start timeout")
              }
            }
          }
        }
        // process.waitFor(applicationCheckInterval, TimeUnit.MILLISECONDS)
        _applicationInfo = currentApplicationInfo
        info(s"Batch[$batchId] has not started sleep [${applicationCheckInterval}ms]")
        Thread.sleep(applicationCheckInterval)
        if (startTimeout(currentTime)) {
          error(s"Batch[$batchId] has not started and timeout so terminated outer")
          throw new KyuubiException("Batch job start timeout")
        }
      }
      collectRemoteLog()
      if (applicationFailed(_applicationInfo)) {
        process.destroyForcibly()
        app = _applicationInfo
        error(s"Batch[$batchId] process failed and destroy it curApp: $app")
        throw new KyuubiException(s"Batch job failed: $app")
      } else {
        process.waitFor()
        val code = process.exitValue()
        if (code != 0) {
          error(s"Batch[$batchId] exitValue not 0: [$code]")
          throw new KyuubiException(s"Process exit with value [$code]")
        }

        Option(_applicationInfo.map(_.id)).foreach {
          case Some(appId) => monitorBatchJob(appId)
          case _ => error(s"Batch[$batchId] has not appId so can not monitor job")
        }
      }
    } finally {
      builder.close()
      cleanupUploadedResourceIfNeeded()
    }
  }

  protected def monitorBatchJob(appId: String): Unit = {
    info(s"Monitoring submitted $batchType batch[$batchId] job: $appId")
    if (_applicationInfo.isEmpty) {
      _applicationInfo = currentApplicationInfo
    }
    if (state == OperationState.PENDING) {
      setStateIfNotCanceled(OperationState.RUNNING)
    }
    var app = _applicationInfo
    debug(s"Monitoring batch[$batchId] curApp: $app")
    if (_applicationInfo.isEmpty) {
      info(s"The $batchType batch[$batchId] job: $appId not found, assume that it has finished.")
    } else if (applicationFailed(_applicationInfo)) {
      error(s"The $batchType batch[$batchId] job: $appId failed.")
      throw new KyuubiException(s"$batchType batch[$batchId] job failed: $app")
    } else {
      updateBatchMetadata()
      while (_applicationInfo.isDefined && !applicationTerminated(_applicationInfo)) {
        Thread.sleep(applicationCheckInterval)
        val newApplicationStatus = currentApplicationInfo
        debug(s"Monitoring batch[$batchId] newApp: $newApplicationStatus")
        if (newApplicationStatus.map(_.state) != _applicationInfo.map(_.state)) {
          app = _applicationInfo
          _applicationInfo = newApplicationStatus
          info(s"Batch report for batch[$batchId], curApp: $app, newApp: $newApplicationStatus")
        } else if (applicationPending(newApplicationStatus) && pendingTimeout(
            System.currentTimeMillis())) {
          error(s"Monitoring batch[$batchId] curApp: $newApplicationStatus pending " +
            s"timeout so terminated it.")
          throw new KyuubiException(s"$batchType job pending timeout: $newApplicationStatus")
        }
      }

      if (applicationFailed(_applicationInfo)) {
        app = _applicationInfo
        error(s"Monitoring batch[$batchId] curApp: $app failed.")
        throw new KyuubiException(s"$batchType job failed: $app")
      }
    }
  }

  protected def collectRemoteLog(): Unit = {
    info(s"CollectRemoteLog $batchType batch[$batchId]")
    val runner: Runnable = () => {
      try {
        while (_applicationInfo.get.state == ApplicationState.PENDING) {
          info(s"CollectRemoteLog batch[$batchId] appState: [${ApplicationState.PENDING}] " +
            s"wait: [${applicationCheckInterval}ms]")
          collectRemoteLogLocker.synchronized {
            collectRemoteLogLocker.wait(applicationCheckInterval)
          }
        }
        info(s"CollectRemoteLog batch[$batchId] start log:")
        val app = _applicationInfo
        if (Seq("SPARK", "PYSPARK").contains(batchType) && app.isDefined) {
          info(s"Application status for ${app.get.id} (phase: Running) Spark context " +
            s"Web UI available at ${app.get.url.orNull}")
        }
        while (true) {
          val appState = _applicationInfo.get.state
          applicationManager.getApplicationLog(
            builder.clusterManager(),
            batchId,
            cluster,
            getGroup) match {
            case Some(input) =>
              info(s"CollectRemoteLog batch[$batchId] got input")
//              var br: BufferedReader = null
              var writer: FileWriter = null
              try {
//                br = new BufferedReader(new InputStreamReader(input))
                writer = new FileWriter(builder.remoteLog)
//                var line = br.readLine()
//                while (true) {
//                  if (null != line) {
//                    writer.write(line + "\n")
//                  } else {
//                    if (ApplicationState.isTerminated(appState)) {
//                      info(
//                        s"CollectRemoteLog batch[$batchId] log line null, " +
//                          s"appState: [$appState] terminated")
//                      return
//                    } else {
//                      info(s"CollectRemoteLog batch[$batchId] log line null, " +
//                        s"appState: [$appState] not terminated wait: " +
//                        s"[$applicationCheckInterval]ms")
//                      collectRemoteLogLocker.synchronized {
//                        collectRemoteLogLocker.wait(applicationCheckInterval)
//                      }
//                    }
//                  }
//
//                  line = br.readLine()
                IOUtils.copy(input, writer, StandardCharsets.UTF_8)
              } catch {
                case e: IOException =>
                  error(s"CollectRemoteLog batch[$batchId] get log failed:", e)
                case _: InterruptedException =>
                  warn(s"CollectRemoteLog batch[$batchId] get log interrupted at inner")
              } finally {
                if (null != writer) {
                  writer.flush()
                  writer.close()
                  writer = null
                }
                if (null != input) {
                  input.close()
                }
              }
              info(s"CollectRemoteLog batch[$batchId] get log finished")
              return
            case None =>
              if (ApplicationState.isTerminated(appState)) {
                error(s"CollectRemoteLog batch[$batchId] get log None," +
                  s" appState: [$appState] terminated")
                return
              } else {
                error(s"CollectRemoteLog batch[$batchId] get log None," +
                  s" appState: [$appState] not terminated, wait [${applicationCheckInterval}ms]")
                collectRemoteLogLocker.synchronized {
                  collectRemoteLogLocker.wait(applicationCheckInterval)
                }
              }
          }
        }
      } catch {
        case _: InterruptedException =>
          warn(s"CollectRemoteLog batch[$batchId] get log interrupted at outer")
          return
      }

    }

    collectRemoteLogThread = PROC_LOGGER.newThread(runner)
    collectRemoteLogThread.start()
  }

  def getOperationLogRowSet(
      order: FetchOrientation,
      from: Int,
      size: Int): TRowSet = {
    val operationLog = getOperationLog
    operationLog.map(_.read(from, size)).getOrElse {
      throw KyuubiSQLException(s"Batch ID: $batchId, failed to generate operation log")
    }
  }

  override def close(): Unit = state.synchronized {
    if (!isClosedOrCanceled) {
      info(s"Close batch[$batchId]")
      try {
        getOperationLog.foreach(_.close())
      } catch {
        case e: IOException => error(e.getMessage, e)
      }

      MetricsSystem.tracing(_.decCount(MetricRegistry.name(OPERATION_OPEN, opType)))

      // fast fail
      if (isTerminalState(state)) {
        killMessage = (false, s"batch $batchId is already terminal so can not kill it.")
        builder.close()
        cleanupUploadedResourceIfNeeded()
        return
      }

      info(s"Close killBatchApplication batch[$batchId]")
      try {
        killMessage = killBatchApplication()
        builder.close()
        cleanupUploadedResourceIfNeeded()
      } finally {
        if (state == OperationState.INITIALIZED) {
          // if state is INITIALIZED, it means that the batch submission has not started to run, set
          // the state to CANCELED manually and cregardless of kill result
          setState(OperationState.CANCELED)
          updateBatchMetadata()
        } else {
          if (killMessage._1 && !isTerminalState(state)) {
            // kill success and we can change state safely
            // note that, the batch operation state should never be closed
            setState(OperationState.CANCELED)
            updateBatchMetadata()
          } else if (killMessage._1) {
            // we can not change state safely
            killMessage = (false, s"batch $batchId is already terminal so can not kill it.")
          } else if (!isTerminalState(state)) {
            // failed to kill, the kill message is enough
          }
        }
      }
    }
  }

  override def cancel(): Unit = {
    throw new IllegalStateException("Use close instead.")
  }

  override def isTimedOut: Boolean = false

  override protected def eventEnabled: Boolean = true

  private def cleanupUploadedResourceIfNeeded(): Unit = {
    if (session.isResourceUploaded) {
      try {
        Files.deleteIfExists(Paths.get(resource))
      } catch {
        case e: Throwable => error(s"Error deleting the uploaded resource: $resource", e)
      }
    }
  }
}

object BatchJobSubmission {

  val PROC_LOGGER = new NamedThreadFactory("process-logger", daemon = true)

  def applicationFailed(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isFailed)
  }

  def applicationTerminated(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isTerminated)
  }

  def applicationPending(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isPending)
  }

  def applicationRunning(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isRunning)
  }

  def applicationCompleted(applicationStatus: Option[ApplicationInfo]): Boolean = {
    applicationStatus.map(_.state).exists(ApplicationState.isCompleted)
  }
}
