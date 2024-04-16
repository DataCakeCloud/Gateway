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

package org.apache.kyuubi.engine.trino

import java.io.File
import java.nio.file.Paths
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.AuthUtil

class TrinoProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    override val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None,
    override val cluster: Option[Cluster] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, conf: KyuubiConf) {
    this(proxyUser, conf, "")
  }

  override protected def module: String = "kyuubi-trino-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.trino.TrinoSqlEngine"

  override protected val commands: Array[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    cluster match {
      case Some(clu) =>
        if (conf.get(ENGINE_TRINO_CONNECTION_URL).isEmpty) {
          conf.set(ENGINE_TRINO_CONNECTION_URL, clu.getMaster)
        }
        if (conf.get(ENGINE_TRINO_CONNECTION_CATALOG).isEmpty) {
          conf.set(ENGINE_TRINO_CONNECTION_CATALOG, clu.getCatalog)
        }
        if (conf.get(ENGINE_TRINO_SERVER_NAME).isEmpty && StringUtils.isNotEmpty(
            clu.getServerName)) {
          conf.set(ENGINE_TRINO_SERVER_NAME, clu.getServerName)
        }
      case None =>
    }

    require(
      conf.get(ENGINE_TRINO_CONNECTION_URL).nonEmpty,
      s"Trino server url can not be null! Please set ${ENGINE_TRINO_CONNECTION_URL.key}")
    require(
      conf.get(ENGINE_TRINO_CONNECTION_CATALOG).nonEmpty,
      s"Trino default catalog can not be null! Please set ${ENGINE_TRINO_CONNECTION_CATALOG.key}")

    conf.get(AUTHENTICATION_METHOD).head match {
      case AuthUtil.AUTHENTICATION_METHOD_KERBEROS =>
        val principal = conf.get(SERVER_PRINCIPAL).getOrElse("")
        conf.set(KYUUBI_SESSION_USER_KEY, principal)
      case _ =>
        val tenant = conf.get(KYUUBI_SESSION_TENANT)
        if (StringUtils.isNotEmpty(tenant)) {
          conf.set(KYUUBI_SESSION_USER_KEY, s"$tenant#$proxyUser")
        } else {
          conf.set(KYUUBI_SESSION_USER_KEY, s"$proxyUser")
        }
    }

    val buffer = new ArrayBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_TRINO_MEMORY)
    buffer += s"-Xmx$memory"
    val javaOptions = conf.get(ENGINE_TRINO_JAVA_OPTIONS)
    if (javaOptions.isDefined) {
      buffer += javaOptions.get
    }

    buffer += "-cp"
    val classpathEntries = new util.LinkedHashSet[String]
    // trino engine runtime jar
    mainResource.foreach(classpathEntries.add)

    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      if (Utils.isTesting) {
        // add dev classpath
        val trinoDeps = parent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        classpathEntries.add(s"$trinoDeps${File.separator}*")
      } else {
        // add prod classpath
        classpathEntries.add(s"$parent${File.separator}*")
      }
    }

    val extraCp = conf.get(ENGINE_TRINO_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)

    buffer += classpathEntries.asScala.mkString(File.pathSeparator)
    buffer += mainClass

    // TODO: How shall we deal with proxyUser,
    // user.name
    // kyuubi.session.user
    // or just leave it, because we can handle it at operation layer
    // buffer += "--conf"
    // buffer += s"$KYUUBI_SESSION_USER_KEY=$proxyUser"

    for ((k, v) <- conf.getAll) {
      if (k != AUTHENTICATION_METHOD.key) {
        buffer += "--conf"
        buffer += s"$k=$v"
      }
    }
    buffer.toArray
  }

  override def shortName: String = "trino"

  override def toString: String = Utils.redactCommandLineArgs(conf, commands).mkString("\n")
}
