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

import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.engine.{ApplicationInfo, ProcBuilder}
import org.apache.kyuubi.engine.spark.{ScriptBatchProcessBuilder, TFJobBatchProcessBuilder}
import org.apache.kyuubi.operation.BatchJobSubmission.applicationCompleted
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiBatchSessionImpl

class ScriptJobSubmission(
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
      case Some("SCRIPT") =>
        new ScriptBatchProcessBuilder(
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
      case Some("TFJOB") =>
        new TFJobBatchProcessBuilder(
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
      case _ =>
        throw new UnsupportedOperationException(s"Batch type $batchType unsupported")
    }
  }

  override protected def currentApplicationInfo: Option[ApplicationInfo] = {
    var applicationInfo = _applicationInfo
    debug(s"CurrentApplicationInfo batch[$batchId] curApp: $applicationInfo")
    if (applicationCompleted(applicationInfo) && applicationInfo.nonEmpty) return applicationInfo
    applicationInfo =
      applicationManager.getApplicationInfo(
        builder.clusterManager(),
        batchId,
        cluster,
        getGroup)
    debug(s"CurrentApplicationInfo batch[$batchId] newApp: $applicationInfo")
    applicationInfo.foreach { info =>
      if (_appStartTime <= 0) {
        _appStartTime = System.currentTimeMillis()
      }
      // script job id is empty, so assign it
      if (Option(info.name).isDefined) {
        info.id = batchId
      }
    }
    applicationInfo
  }

  override protected def collectRemoteLog(): Unit = {
    info(s"CollectRemoteLog $batchType batch[$batchId]")
    Option(batchType).map(_.toUpperCase(Locale.ROOT)) match {
      case Some("SCRIPT") =>
        super.collectRemoteLog()
      case _ =>
        info(s"CollectRemoteLog $batchType batch[$batchId] dependency cli")
    }
  }

}
