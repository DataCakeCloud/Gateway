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

import java.io.File
import java.nio.file.Paths

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{KYUUBI_CONF_DIR, KYUUBI_HOME}
import org.apache.kyuubi.engine.KubernetesApplicationOperation._
import org.apache.kyuubi.engine.spark.SparkProcessBuilder.{JOB_ID_LABEL_KEY, JOB_NAME_LABEL_KEY}
import org.apache.kyuubi.operation.log.OperationLog

class TFJobBatchProcessBuilder(
    proxyUser: String,
    conf: KyuubiConf,
    batchId: String,
    batchName: String,
    mainResource: Option[String],
    mainClass: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    extraEngineLog: Option[OperationLog],
    cluster: Option[Cluster] = None)
  extends ScriptBatchProcessBuilder(
    proxyUser,
    conf,
    batchId,
    batchName,
    mainResource,
    mainClass,
    batchConf,
    batchArgs,
    extraEngineLog,
    cluster) {

  override def shortName: String = "tfjob"

  override protected def module: String = "kyuubi-tfjob-batch-submit"

  override protected def executable: String = {
    Paths.get("bin/tf-operator").toFile.getCanonicalPath
  }

  private val clusterConfigFile = env.get(KYUUBI_CONF_DIR)
    .orElse(env.get(KYUUBI_HOME).map(_ + File.separator + "conf"))
    .map(d => new File(d + File.separator + "cluster_config.json"))
    .filter(_.exists()).map(_.getAbsolutePath).orNull

  override protected lazy val commands: Array[String] = cluster match {
    case Some(clu) =>
      val buffer = new ArrayBuffer[String]()
      buffer += executable
      buffer += "--cluster-config"
      buffer += clusterConfigFile
      buffer += "--kube-config"
      buffer += clu.getKubeConfigFilePath
      buffer += "--job-config"
      assert(mainResource.isDefined)
      buffer += mainResource.get

      // tag batch application
      buffer += "--labels"
      buffer += String.join(
        ",",
        LABEL_KYUUBI_UNIQUE_KEY + "=" + batchId,
        LABEL_KYUUBI_APP_KEY + "=" + LABEL_KYUUBI_APP_VALUE,
        JOB_ID_LABEL_KEY + "=" + batchId,
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

}
