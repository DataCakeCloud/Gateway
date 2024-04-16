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

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.{LAKECAT_CATALOG_PROBE_CLUSTER, LAKECAT_CATALOG_PROBE_REAL_USER, SPARK_SQL_DEFAULT_DB_KEY}
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.operation.log.OperationLog

class SparkBatchProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    batchId: String,
    batchName: String,
    override val mainResource: Option[String],
    override val mainClass: String,
    batchConf: Map[String, String],
    batchArgs: Seq[String],
    override val extraEngineLog: Option[OperationLog],
    cluster: Option[Cluster] = None)
  extends SparkProcessBuilder(proxyUser, conf, batchId, extraEngineLog, cluster) {
  import SparkProcessBuilder._

  override protected val commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    Option(mainClass).foreach { cla =>
      buffer += CLASS
      buffer += cla
    }
    // set conf front of user's to avoid overwritten by smy
    addClusterConf(buffer)

    addLakeLineageConf(buffer)

    addDatabase(buffer)

    addAuthentication(buffer)

    val batchKyuubiConf = new KyuubiConf(false)
    // complete `spark.master` if absent on kubernetes
    completeMasterUrl(batchKyuubiConf)
    batchConf.foreach(entry => { batchKyuubiConf.set(entry._1, entry._2) })
    // tag batch application
    KyuubiApplicationManager.tagApplication(batchId, "spark", clusterManager(), batchKyuubiConf)

    if (!batchConf.contains(SPARK_WAIT_COMPLETION_KEY)) {
      batchKyuubiConf.set(SPARK_WAIT_COMPLETION_KEY, "false")
    }

    (batchKyuubiConf.getAll ++ sparkAppNameConf()).foreach { case (k, v) =>
      buffer += CONF
      buffer += s"${convertConfigKey(k)}=$v"
    }

    setSparkUserName(proxyUser, buffer)
    // buffer += PROXY_USER
    // buffer += proxyUser

    assert(mainResource.isDefined)
    buffer += mainResource.get

    batchArgs.foreach { arg => buffer += arg }

    buffer.toArray
  }

  override protected[kyuubi] def addNamedConf(buffer: ArrayBuffer[String]): Unit = {
    super.addNamedConf(buffer)
    buffer += CONF
    buffer += SPARK_DRIVER_LABEL_PREFIX + JOB_NAME_LABEL_KEY + "=" + batchName
    buffer += CONF
    buffer += SPARK_EXECUTOR_LABEL_PREFIX + JOB_NAME_LABEL_KEY + "=" + batchName
  }

  override protected[kyuubi] def addLakeLineageConf(buffer: ArrayBuffer[String]): Unit = {
    cluster match {
      case Some(clu) =>
        buffer += CONF
        buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_CLUSTER + "=" + clu.getId
      case _ =>
    }
    buffer += CONF
    buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_REAL_USER + "=" + proxyUser
    // TODO disable param
    // buffer += CONF
    // buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_USER_GROUP + "=" + ""
  }

  override protected[kyuubi] def addDatabase(buffer: ArrayBuffer[String]): Unit = {
    if (!batchConf.contains(SPARK_SQL_DEFAULT_DB_KEY)) {
      super.addDatabase(buffer)
    }
  }

  private def sparkAppNameConf(): Map[String, String] = {
    Option(batchName).filterNot(_.isEmpty).map { appName =>
      Map(APP_KEY -> appName)
    }.getOrElse(Map())
  }

  override protected def module: String = "kyuubi-spark-batch-submit"

  override def clusterManager(): Option[String] = cluster match {
    case Some(cluster) => Some(cluster.getMaster)
    case _ => batchConf.get(MASTER_KEY).orElse(defaultMaster)
  }
}
