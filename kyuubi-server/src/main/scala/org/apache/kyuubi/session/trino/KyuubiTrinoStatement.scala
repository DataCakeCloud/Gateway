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

package org.apache.kyuubi.session.trino

import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.google.common.base.Verify
import io.trino.client._
import okhttp3.OkHttpClient

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_DATABASE, ENGINE_TRINO_CONNECTION_CATALOG, ENGINE_TRINO_SHOW_PROGRESS, ENGINE_TRINO_SHOW_PROGRESS_DEBUG}
import org.apache.kyuubi.operation.log.OperationLog

/**
 * Trino client communicate with trino cluster.
 */
class KyuubiTrinoStatement(
    httpclient: OkHttpClient,
    clientSessionRef: AtomicReference[ClientSession],
    kyuubiConf: KyuubiConf,
    sql: String,
    operationLog: Option[OperationLog]) extends Logging {

  private lazy val trino = StatementClientFactory
    .newStatementClient(httpclient, clientSessionRef.get(), sql)

  private lazy val showProcess = kyuubiConf.get(ENGINE_TRINO_SHOW_PROGRESS)
  private lazy val showDebug = kyuubiConf.get(ENGINE_TRINO_SHOW_PROGRESS_DEBUG)

  def getTrinoClient: StatementClient = trino

  def getCurrentCatalog: String = kyuubiConf.get(ENGINE_TRINO_CONNECTION_CATALOG).orNull

  def getCurrentDatabase: String = kyuubiConf.get(ENGINE_JDBC_CONNECTION_DATABASE)

  def getColumns: List[Column] = {
    while (trino.isRunning) {
      val results = trino.currentStatusInfo()
      val columns = results.getColumns
      if (columns != null) {
        info(s"Execute with Trino query id: ${results.getId}")
        return columns.asScala.toList
      }
      trino.advance()
    }
    Verify.verify(trino.isFinished)
    val finalStatus = trino.finalStatusInfo()
    if (finalStatus.getError == null) {
      throw KyuubiSQLException(s"Query has no columns (#${finalStatus.getId})")
    } else {
      throw KyuubiSQLException(
        s"Query failed (#${finalStatus.getId}): ${finalStatus.getError.getMessage}")
    }
  }

  def getQueryId: String = {
    val info = trino.currentStatusInfo()
    if (null != info) {
      return info.getId
    }
    ""
  }

  def execute(): Iterator[List[Any]] = {
    Iterator.continually {
      @tailrec
      def getData: (Boolean, List[List[Any]]) = {
        if (trino.isRunning) {
          val data = trino.currentData().getData
          trino.advance()
          if (data != null) {
            (true, data.asScala.toList.map(_.asScala.toList))
          } else {
            getData
          }
        } else {
          Verify.verify(trino.isFinished)
          if (operationLog.isDefined && showProcess) {
            KyuubiTrinoStatusPrinter.printFinalInfo(trino, operationLog.get, showDebug)
          }
          val finalStatus = trino.finalStatusInfo()
          if (finalStatus.getError != null) {
            throw KyuubiSQLException(
              s"Query ${finalStatus.getId} failed: ${finalStatus.getError.getMessage}")
          }
          updateTrinoContext()
          (false, List[List[Any]]())
        }
      }
      getData
    }
      .takeWhile(_._1)
      .flatMap(_._2)
  }

  def updateTrinoContext(): Unit = {
    val session = clientSessionRef.get()

    var builder = ClientSession.builder(session)
    // update catalog and schema
    if (trino.getSetCatalog.isPresent || trino.getSetSchema.isPresent) {
      builder = builder
        .withCatalog(trino.getSetCatalog.orElse(session.getCatalog))
        .withSchema(trino.getSetSchema.orElse(session.getSchema))
    }

    // update path if present
    if (trino.getSetPath.isPresent) {
      builder = builder.withPath(trino.getSetPath.get)
    }

    // update session properties if present
    if (!trino.getSetSessionProperties.isEmpty || !trino.getResetSessionProperties.isEmpty) {
      val properties = session.getProperties.asScala.clone()
      properties ++= trino.getSetSessionProperties.asScala
      properties --= trino.getResetSessionProperties.asScala
      builder = builder.withProperties(properties.asJava)
    }

    clientSessionRef.set(builder.build())
  }
}

object KyuubiTrinoStatement {
  def apply(
      httpClient: OkHttpClient,
      clientSession: ClientSession,
      kyuubiConf: KyuubiConf,
      sql: String,
      operationLog: Option[OperationLog] = None): KyuubiTrinoStatement = {
    new KyuubiTrinoStatement(
      httpClient,
      new AtomicReference(clientSession),
      kyuubiConf,
      sql,
      operationLog)
  }
}
