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

import java.util.Locale

import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.engine.{ApplicationInfo, KillResponse, ProcBuilder}
import org.apache.kyuubi.engine.ResourceType.DEPLOYMENT
import org.apache.kyuubi.engine.flink.FlinkBatchProcessBuilder
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiBatchSessionImpl

class FlinkJobSubmission(
    session: KyuubiBatchSessionImpl,
    batchType: String,
    batchName: String,
    resource: String,
    className: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    recoveryMetadata: Option[Metadata],
    cluster: Option[Cluster] = None)
  extends BatchJobSubmission(
    session,
    batchType,
    batchName,
    resource,
    className,
    batchConf,
    batchArgs,
    recoveryMetadata,
    cluster) {

  @VisibleForTesting
  override protected[kyuubi] lazy val builder: ProcBuilder = {
    Option(batchType).map(_.toUpperCase(Locale.ROOT)) match {
      case Some("FLINK") =>
        new FlinkBatchProcessBuilder(
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
          .resetFlinkHome()
          .setProcEnv()
          .setProcAuthEnv(cluster.orNull)
      case _ =>
        throw new UnsupportedOperationException(s"Batch type $batchType unsupported")
    }
  }

  override protected def currentApplicationInfo: Option[ApplicationInfo] = {
    var applicationInfo = _applicationInfo
    debug(s"CurrentApplicationInfo batch[$batchId] curApp: $applicationInfo")
    applicationInfo =
      applicationManager.getApplicationInfo(
        builder.clusterManager(),
        batchId,
        cluster,
        getGroup,
        Some(DEPLOYMENT)).filter(_.id != null)
    debug(s"CurrentApplicationInfo batch[$batchId] newApp: $applicationInfo")
    applicationInfo.foreach { _ =>
      if (_appStartTime <= 0) {
        _appStartTime = System.currentTimeMillis()
      }
    }
    _applicationInfo = applicationInfo
    applicationInfo
  }

  override def getOrFetchCurrentApplicationInfo: Option[ApplicationInfo] = currentApplicationInfo

  override protected def runInternal(): Unit = session.handleSessionException {
    val asyncOperation: Runnable = () => {
      try {
        info(s"Submitting $batchType batch[$batchId] job:\n$builder")
        debug(s"Launching engine env:\n${builder.getProcEnvs}")
        val process = builder.start
        process.waitFor()
        val code = process.exitValue()
        if (code != 0) {
          error(s"Batch[$batchId] exitValue not 0: [$code]")
          killBatchApplication()
          throw new KyuubiException(s"Process exit with value [$code]")
        }
        Option(_applicationInfo.map(_.id)).foreach {
          case Some(appId) => monitorBatchJob(appId)
          case _ => error(s"Batch[$batchId] has not appId so can not monitor job")
        }
        setState(OperationState.RUNNING)
      } catch {
        onError()
      } finally {
        updateBatchMetadata()
        info(s"Batch[$batchId] finished")
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

  override protected def killBatchApplication(): KillResponse = {
    val (killed, msg) = applicationManager.killApplication(
      builder.clusterManager(),
      batchId,
      cluster,
      getGroup,
      Some(DEPLOYMENT))
    try {
      builder.clean()
    } catch {
      case e: Exception =>
        error("KillBatchApplication batch[$batchId] builder clean failed", e)
    }
    (killed, msg)
  }
}
