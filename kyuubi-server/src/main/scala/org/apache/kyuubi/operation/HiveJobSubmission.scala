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

import java.io.FileWriter
import java.sql.DriverManager

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_METHOD, ENGINE_JDBC_CONNECTION_DATABASE, KYUUBI_SESSION_GROUP, SERVER_PRINCIPAL}
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationState, ProcBuilder}
import org.apache.kyuubi.engine.jdbc.JdbcProcessBuilder
import org.apache.kyuubi.jdbc.hive.{KyuubiConnection, KyuubiStatement}
import org.apache.kyuubi.operation.BatchJobSubmission.PROC_LOGGER
import org.apache.kyuubi.operation.OperationState._
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiBatchSessionImpl
import org.apache.kyuubi.util.{AuthUtil, SqlUtils}

class HiveJobSubmission(
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
  override protected[kyuubi] lazy val builder = new JdbcProcessBuilder(
    session.user,
    session.sessionConf,
    batchId,
    getOperationLog,
    cluster)

  private var connOp: Option[KyuubiConnection] = None
  private var stmtOp: Option[KyuubiStatement] = None
  @volatile private var collectRemoteLogStopped = true

  private val connectionUrl: String = cluster match {
    case Some(clu) =>
      var master = clu.getMaster
      val database = batchConf.getOrElse(
        ENGINE_JDBC_CONNECTION_DATABASE.key,
        ENGINE_JDBC_CONNECTION_DATABASE.defaultValStr)
      if (!master.endsWith("/")) {
        master += "/"
      }
      master += database

      session.sessionConf.get(AUTHENTICATION_METHOD).head match {
        case AuthUtil.AUTHENTICATION_METHOD_KERBEROS =>
          val group = session.sessionConf.get(KYUUBI_SESSION_GROUP)
          val principal = session.sessionConf.get(SERVER_PRINCIPAL).orNull
          master += s";auth=${AuthUtil.AUTHENTICATION_METHOD_KERBEROS};" +
            s"user=$group;principal=$principal;"
        case _ =>
      }
      master
    case _ => throw new KyuubiException("cluster is empty")
  }

  override protected def currentApplicationInfo: Option[ApplicationInfo] = {
    Some(ApplicationInfo(
      id = session.handle.identifier.toString,
      name = batchName,
      state = state match {
        case INITIALIZED | PENDING | RUNNING =>
          ApplicationState.RUNNING
        case COMPILED | FINISHED | CANCELED | CLOSED =>
          ApplicationState.FINISHED
        case TIMEOUT | ERROR | UNKNOWN | _ =>
          ApplicationState.FAILED
      },
      url = Some(connectionUrl),
      clusterId = cluster.map(clu => clu.getId)))
  }

  override protected def runInternal(): Unit = session.handleSessionException {
    val asyncOperation: Runnable = () => {
      try {
        val sql = SqlUtils.parseSqlFromArgs(batchArgs)
        if (StringUtils.isEmpty(sql)) {
          throw new KyuubiException("parse sql failed")
        }
        execute(sql)
      } catch {
        onError()
      } finally {
        close()
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

  def execute(sql: String): Unit = {
    val conn = DriverManager.getConnection(connectionUrl).asInstanceOf[KyuubiConnection]
    connOp = Some(conn)
    val stmt = conn.createStatement().asInstanceOf[KyuubiStatement]
    stmtOp = Some(stmt)
    stmt.executeAsync(sql)
    _appStartTime = System.currentTimeMillis()
    setState(RUNNING)
    collectRemoteLog()
    stmt.getResultSet
    lastAccessTime = System.currentTimeMillis()
    setState(FINISHED)
  }

  override def cancel(): Unit = {
    try {
      stmtOp.foreach { st =>
        st.cancel()
      }
    } finally {
      close()
    }
  }

  override def close(): Unit = {
    try {
      stmtOp.foreach { st =>
        if (!collectRemoteLogStopped) {
          info(s"Close session sleep: [$applicationCheckInterval]ms, " +
            s"reason collectRemoteLog is running")
          Thread.sleep(applicationCheckInterval)
        }
        collectRemoteLogStopped = true
        st.close()
      }
    } finally {
      connOp.foreach { c =>
        c.close()
      }
    }
    killMessage = (true, "SUCCESS")
  }

  override protected def collectRemoteLog(): Unit = {
    collectRemoteLogStopped = false
    info(s"CollectRemoteLog $batchType batch[$batchId]")
    val remoteLogWriter = new FileWriter(ProcBuilder.findEngineLog(_operationLog, batchId)._1)
    val runner: Runnable = () => {
      try {
        info(s"CollectRemoteLog batch[$batchId] start log:")
        while (!collectRemoteLogStopped) {
          stmtOp.foreach { stmt =>
            if (!stmt.isClosed && stmt.hasMoreLogs) {
              stmt.getExecLog.forEach { log =>
                IOUtils.write(log + "\n", remoteLogWriter)
              }
            }
          }
        }
      } catch {
        case _: InterruptedException =>
          warn(s"CollectRemoteLog batch[$batchId] collect log interrupted")
          return
        case e: Exception =>
          error(s"CollectRemoteLog batch[$batchId] collect log failed", e)
          return
      } finally {
        remoteLogWriter.close()
        collectRemoteLogStopped = true
      }
    }

    collectRemoteLogThread = PROC_LOGGER.newThread(runner)
    collectRemoteLogThread.start()
  }

}
