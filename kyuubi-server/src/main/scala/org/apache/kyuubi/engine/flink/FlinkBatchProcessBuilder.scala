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

package org.apache.kyuubi.engine.flink

import java.nio.file.Paths

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi._
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_METHOD, SERVER_KEYTAB, SERVER_PRINCIPAL}
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.engine.ProcBuilder.{HADOOP_CLASSPATH, HADOOP_HOME}
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder.{CLASS, FLINK_SUBMIT_FILE}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.AuthUtil

class FlinkBatchProcessBuilder(
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
  extends ProcBuilder with Logging {
  import org.apache.kyuubi.engine.flink.FlinkBatchProcessBuilder._

  val flinkHome: String = getEngineHome(shortName)
  val hadoopHome: String = super.getEngineHome("hadoop")

  override def shortName: String = "flink"

  override protected def module: String = "kyuubi-flink-engine"

  override protected def engineRefId: String = batchId

  override def getEngineHome(shortName: String): String = cluster match {
    case Some(clu) =>
      val homeKey = s"${shortName.toUpperCase}_HOME"
      conf.getEnvs.get(homeKey) match {
        case Some(homeVal) => homeVal + "/" + clu.getCommand.getCommand
        case None => throw validateEnv(homeKey)
      }
    case _ => super.getEngineHome(shortName)
  }

  override protected val executable: String = {
    Paths.get(flinkHome, "bin", FLINK_SUBMIT_FILE).toFile.getCanonicalPath
  }

  private[kyuubi] def resetFlinkHome(): FlinkBatchProcessBuilder = {
    setProcEnv(FlinkBatchProcessBuilder.FLINK_HOME, flinkHome)
    this
  }

  override def setProcEnv(): FlinkBatchProcessBuilder = cluster match {
    case Some(clu) =>
      clu.getClusterType match {
        case "k8s" =>
        case "yarn" =>
          setProcEnv(HADOOP_HOME, hadoopHome)
          setProcEnv(
            HADOOP_CLASSPATH,
            clu.getYarnClusterConfigPath + ":" + conf.getEnvs.get(HADOOP_CLASSPATH).orNull)
        case _ =>
      }

      val env = clu.getCommand.getEnv
      if (null != env && !env.isEmpty) {
        env.forEach(a => {
          val ss = a.split("=")
          if (ss.length >= 2) {
            setProcEnv(ss(0).trim, ss(1).trim)
          }
        })
      }
      this
    case _ => this
  }

  override protected val commands: Array[String] = cluster match {
    case Some(clu) =>
      val batchKyuubiConf = new KyuubiConf(false)
      batchConf.foreach(entry => {
        batchKyuubiConf.set(entry._1, entry._2)
      })
      KyuubiApplicationManager.tagApplication(batchId, shortName, clusterManager(), batchKyuubiConf)
      val buffer = new ArrayBuffer[String]()
      buffer += executable

      val mode = batchKyuubiConf.get(KyuubiConf.ENGINE_FLINK_APPLICATION_MODE)

      buffer += mode

      Option(mainClass).foreach { cla =>
        buffer += CLASS
        buffer += cla
      }

      val dConf = mutable.Map(batchKyuubiConf.getAll.toSeq: _*)

      buffer += "-t"
      clu.getClusterType match {
        case "k8s" =>
          buffer += "kubernetes-application"
          dConf += ("kubernetes.config.file" -> clu.getKubeConfigFilePath)
        case "yarn" =>
          buffer += "yarn-application"
        case _ =>
      }

      addAuthentication(buffer)

      dConf.foreach(entry => {
        buffer += s"$CONF${entry._1}=${entry._2}"
      })

      batchArgs.foreach { arg => buffer += arg }

      if (mainResource.nonEmpty) {
        buffer += mainResource.get
      }

      buffer.toArray
    case None => Array.empty
  }

  protected[kyuubi] def addAuthentication(buffer: ArrayBuffer[String]): Unit = {
    conf.get(AUTHENTICATION_METHOD).head match {
      case AuthUtil.AUTHENTICATION_METHOD_KERBEROS =>
        val principal = conf.get(SERVER_PRINCIPAL).orNull
        val keytab = conf.get(SERVER_KEYTAB).orNull
        buffer += s"$CONF$PRINCIPAL=$principal"
        buffer += s"$CONF$KEYTAB=$keytab"
        buffer += s"$CONF$LOGIN_CONTEXTS=$LOGIN_CONTEXTS_CLIENT"
      case _ =>
    }
  }

  override def clusterManager(): Option[String] = cluster match {
    case Some(cluster) => Some(cluster.getMaster)
    case _ => None
  }

  override def toString: String = {
    if (commands == null) {
      super.toString
    } else {
      Utils.redactCommandLineArgs(conf, commands).map {
        case arg if arg.startsWith("-") => s"\\\n\t$arg"
        case arg => arg
      }.mkString(" ")
    }
  }
}

object FlinkBatchProcessBuilder extends Logging {
  val FLINK_HOME = "FLINK_HOME"
  val CONF = "-D"
  val KEYTAB = "security.kerberos.login.keytab"
  val PRINCIPAL = "security.kerberos.login.principal"
  val LOGIN_CONTEXTS = "security.kerberos.login.contexts"
  val LOGIN_CONTEXTS_CLIENT = "Client"
}
