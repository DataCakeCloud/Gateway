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

package org.apache.kyuubi.engine.spark

import java.nio.file.Paths

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.KubernetesApplicationOperation._
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder.{JOB_ID_LABEL_KEY, JOB_NAME_LABEL_KEY}
import org.apache.kyuubi.operation.log.OperationLog

class ScriptBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    override val engineRefId: String,
    batchName: String,
    override val mainResource: Option[String],
    val mainClass: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    override val extraEngineLog: Option[OperationLog],
    override val cluster: Option[Cluster] = None)
  extends ProcBuilder {

  override def shortName: String = "script"

  override protected def module: String = "kyuubi-script-batch-submit"

  override protected def executable: String = {
    Paths.get("bin/kubernetes-pod-operator").toFile.getCanonicalPath
  }

  override protected lazy val commands: Array[String] = cluster match {
    case Some(clu) =>
      val buffer = new ArrayBuffer[String]()
      buffer += executable
      buffer += "--kube-config"
      buffer += clu.getKubeConfigFilePath
      buffer += "--pod-config"
      assert(mainResource.isDefined)
      buffer += mainResource.get
      buffer += "--startup-timeout-seconds"
      buffer += "7200"
      buffer += "--delete-pod"
      buffer += "--get-logs"

      // tag batch application
      buffer += "--labels"
      buffer += String.join(
        ",",
        LABEL_KYUUBI_UNIQUE_KEY + "=" + engineRefId,
        LABEL_KYUUBI_APP_KEY + "=" + LABEL_KYUUBI_APP_VALUE,
        JOB_ID_LABEL_KEY + "=" + engineRefId,
        JOB_NAME_LABEL_KEY + "=" + batchName)
      /* not support custom params
      batchConf.foreach {
        case (k, v) =>
          buffer += s"${convertConfigKey(k)}=$v"
          buffer += v
      }
       */
      batchArgs.foreach { arg => buffer += arg }

      buffer.toArray
    case None =>
      throw new IllegalArgumentException(s"Build $shortName jod failed reason cluster is null")
  }

  override def clusterManager(): Option[String] = cluster match {
    case Some(clu) => Some(clu.getMaster)
    case None => None
  }

  private def convertConfigKey(key: String): String = {
    if (key.startsWith("--")) {
      key
    } else {
      "--" + key
    }
  }

}
