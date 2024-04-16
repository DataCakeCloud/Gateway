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

package org.apache.kyuubi.server

import java.sql.SQLException
import java.util
import java.util.Base64

import scala.collection.mutable.ListBuffer

import io.lakecat.probe.model.SqlInfo
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.ServerContext

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.cli.Handle
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.ha.client.{KyuubiServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{ExecuteStatement, Operation, OperationHandle}
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.{CURRENT_SERVER_CONTEXT, FeServiceServerContext, OK_STATUS}
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.storage.StorageManager
import org.apache.kyuubi.util.{ObjectStorageUtil, SqlUtils}

final class KyuubiTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("KyuubiTBinaryFrontend") {

  override protected def hadoopConf: Configuration = KyuubiServer.getHadoopConf()

  private val sqlInfoCache: util.Map[String, SqlInfo] = new util.HashMap[String, SqlInfo]()

  private val MASKS = Array[Byte](0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80.toByte)

  private val NULL = "NULL"

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new KyuubiServiceDiscovery(this))
    } else {
      None
    }
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    super.initialize(conf)

    server.foreach(_.setServerEventHandler(new FeTServerEventHandler() {
      override def createContext(input: TProtocol, output: TProtocol): ServerContext = {
        MetricsSystem.tracing { ms =>
          ms.incCount(THRIFT_BINARY_CONN_OPEN)
          ms.incCount(THRIFT_BINARY_CONN_TOTAL)
        }
        new FeServiceServerContext()
      }

      override def deleteContext(
          serverContext: ServerContext,
          input: TProtocol,
          output: TProtocol): Unit = {
        super.deleteContext(serverContext, input, output)
        MetricsSystem.tracing { ms =>
          ms.decCount(THRIFT_BINARY_CONN_OPEN)
        }
        try {
          // close data uploader
          storageManager.foreach { sm =>
            val handle = serverContext.getSessionHandle
            if (null != handle) {
              sm.closeSession(handle.identifier.toString)
              info(s"Session [${handle.identifier.toString}] deleteContext end")
            }
          }
        } catch {
          case e: Exception =>
            error(s"StorageManager closeSession failed: ${e.getMessage}")
        }
      }
    }))
  }

  private def storageManager: Option[StorageManager] = {
    val sm = be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sm.storageManager
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp

    var sessionId: String = null
    try {
      val sessionHandle = getSessionHandle(req, resp)
      val session = be.sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiSession]
      sessionId = session.handle.identifier.toString
      val output = session.sessionConf.get(KYUUBI_SESSION_OUTPUT)
      val region = session.sessionConf.get(KYUUBI_SESSION_OUTPUT_REGION)

      if (needPersistOutput(output)) {
        val os = ObjectStorageUtil.build(output, region, getConf)
        storageManager.foreach { sm =>
          sm.newStorageSession(
            sessionId,
            output,
            session.sessionConf.get(KYUUBI_SESSION_OUTPUT_FILENAME),
            session.sessionConf.get(KYUUBI_SESSION_OUTPUT_FORMAT),
            session.sessionConf.get(KYUUBI_SESSION_OUTPUT_DELIMITER),
            session.sessionConf.get(KYUUBI_SESSION_OUTPUT_FILE_SIZE),
            os)
        }
      }

      val respConfiguration = new java.util.HashMap[String, String]()

      if (session.isInstanceOf[KyuubiSessionImpl]) {
        val launchEngineOp = be.sessionManager.getSession(sessionHandle)
          .asInstanceOf[KyuubiSessionImpl].launchEngineOp
        val opHandleIdentifier = Handle.toTHandleIdentifier(launchEngineOp.getHandle.identifier)
        respConfiguration.put(
          KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID,
          Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getGuid))
        respConfiguration.put(
          KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET,
          Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getSecret))
      }

      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      resp.setConfiguration(respConfiguration)
      resp.setStatus(OK_STATUS)
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        error("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
        storageManager.foreach { sm =>
          if (null != sessionId) {
            sm.cancelSession(sessionId)
          }
        }
    }

    resp
  }

  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    debug(req)
    val sessionHandle = SessionHandle(req.getSessionHandle)
    val sessionId = sessionHandle.identifier.toString
    val session = be.sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiSession]

    info(s"ExecuteStatement: session[$sessionId], [${req.getStatement}]")

    var sqlInfo: SqlInfo = null
    try {
      if (StringUtils.isBlank(req.getStatement)) {
        throw new SQLException(s"Invalid sql, session[$sessionId]: sql: [${req.getStatement}]")
      }

      if (needAuth(session.sessionConf)) {
        sqlInfo = authSql(req.getStatement, session.sessionConf)
        // skip sqlInfo is null
        if (null != sqlInfo && !sqlInfo.getPermit) {
          val error = SqlUtils.packAuthError(
            session.sessionConf.getOption(KYUUBI_SESSION_REAL_USER_KEY).orNull,
            req.getStatement,
            sqlInfo)
          throw new SQLException(error)
        }
      }
    } catch {
      case e: Exception =>
        error(s"Authorization sql failed: ", e)
        storageManager.foreach { sm =>
          sm.cancelSession(sessionId)
        }
        val resp = new TExecuteStatementResp
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
        return resp
    }

    val resp = super.ExecuteStatement(req)
    var operationId: String = null
    try {
      if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
        operationId = OperationHandle(resp.getOperationHandle).identifier.toString
        sqlInfoCache.put(operationId, sqlInfo)
        storageManager.foreach { sm =>
          Option(sm.getSession(sessionId)).foreach {
            session =>
              session.openStorageOperation(operationId)
          }
        }
      } else {
        storageManager.foreach { sm =>
          sm.cancelSession(sessionId)
        }
      }
    } catch {
      case e: Exception =>
        error("Error executing statement: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
        storageManager.foreach { sm =>
          sm.cancelSession(sessionId)
        }
        if (null != operationId) {
          sqlInfoCache.remove(operationId)
        }
    }
    resp
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    val resp = super.GetOperationStatus(req)
    val opHandle = OperationHandle(req.getOperationHandle)
    resp.getOperationState match {
      case TOperationState.FINISHED_STATE =>
        val operation =
          be.sessionManager.operationManager.getOperation(opHandle)
        val session = operation.getSession.asInstanceOf[KyuubiSession]
        if (needAuth(session.sessionConf)) {
          pushSql(operation, sqlInfoCache.remove(opHandle.identifier.toString))
        }
      case TOperationState.ERROR_STATE | TOperationState.CANCELED_STATE
          | TOperationState.CLOSED_STATE | TOperationState.TIMEDOUT_STATE =>
        sqlInfoCache.remove(opHandle.identifier.toString)
      case _ =>
    }
    resp
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    debug(req)
    val operationId = OperationHandle(req.getOperationHandle).identifier.toString
    val resp = super.GetResultSetMetadata(req)
    try {
      if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
        if (resp.getSchema.getColumnsSize <= 0) {
          return resp
        }
        storageManager.foreach { sm =>
          Option(sm.findOperation(operationId)).foreach {
            operation =>
              val header = new StringBuilder()
              var appendDelimiter = false
              val delimiter = operation.delimiter
              resp.getSchema.getColumns.forEach { col =>
                var colName = col.getColumnName
                val idx = colName.indexOf(".")
                if (idx >= 0 && idx < colName.length) {
                  colName = colName.substring(idx + 1)
                }
                if (appendDelimiter) {
                  header.append(delimiter)
                }
                header.append(colName)
                appendDelimiter = true
              }
              operation.writeHeader(header.toString())
          }
        }
      }
    } catch {
      case e: Exception =>
        error("Error get metadata: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
        storageManager.foreach { sm =>
          Option(sm.findSessionByOperation(operationId)).foreach {
            session =>
              session.removeOperation(operationId)
          }
        }
    }
    debug(resp)
    resp
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    debug(req)
    val opHandle = OperationHandle(req.getOperationHandle)
    val operationId = opHandle.identifier.toString
    val operation =
      be.sessionManager.operationManager.getOperation(opHandle)
    val session = operation.getSession.asInstanceOf[KyuubiSession]
    val sessionId = session.handle.identifier.toString
    info(s"FetchResults session[$sessionId] op[$operationId] start")
    val resp = super.FetchResults(req)

    try {
      if (resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS) {
        if (req.getFetchType == 0) {
          storageManager.foreach { sm =>
            Option(sm.findOperation(operationId)).foreach {
              operation =>
                val rs = resp.getResults
                info(s"FetchResults session[$sessionId] op[$operationId] write")
                operation.write(toRows(rs, operation.delimiter).result())
            }
          }
        }
      } else {
        storageManager.foreach { sm =>
          Option(sm.findSessionByOperation(operationId)).foreach {
            session =>
              info(s"FetchResults session[$sessionId] op[$operationId] not success removeOp")
              session.removeOperation(operationId)
          }
        }
      }
    } catch {
      case e: Exception =>
        error(s"FetchResults session[$sessionId] op[$operationId] error: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
        storageManager.foreach { sm =>
          Option(sm.findSessionByOperation(operationId)).foreach {
            session =>
              info(s"FetchResults session[$sessionId] op[$operationId] error removeOp")
              session.removeOperation(operationId)
          }
        }
    }
    info(s"FetchResults session[$sessionId] op[$operationId] end")
    resp
  }

  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    try {
      super.CancelOperation(req)
    } finally {
      val operationId = OperationHandle(req.getOperationHandle).identifier.toString
      storageManager.foreach { sm =>
        Option(sm.findSessionByOperation(operationId)).foreach {
          session =>
            session.removeOperation(operationId)
        }
      }
    }
  }

  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    val sessionHandle = SessionHandle(req.getSessionHandle)
    val sessionId = sessionHandle.identifier.toString
    info(s"CloseSession session[$sessionId]")
    try {
      super.CloseSession(req)
    } finally {
      storageManager.foreach { sm =>
        sm.closeSession(SessionHandle(req.getSessionHandle).identifier.toString)
      }
    }
  }

  override protected def isServer(): Boolean = true

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)
    val resp = new TRenewDelegationTokenResp
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }

  private def needPersistOutput(output: String): Boolean = {
    StringUtils.isNotBlank(output)
  }

  private def getRowSize(rs: TRowSet): Int = {
    if (rs.getColumnsSize > 0) {
      rs.getColumns.get(0).getFieldValue match {
        case t: TStringColumn => t.getValues.size()
        case t: TDoubleColumn => t.getValues.size()
        case t: TI64Column => t.getValues.size()
        case t: TI32Column => t.getValues.size()
        case t: TI16Column => t.getValues.size()
        case t: TBoolColumn => t.getValues.size()
        case t: TByteColumn => t.getValues.size()
        case t: TBinaryColumn => t.getValues.size()
        case _ => 0
      }
    } else rs.getRowsSize
  }

  def isNull(nulls: Array[Byte], index: Int): Boolean = {
    if (nulls.isEmpty) {
      return false
    }
    val idx = index / 8
    if (idx >= nulls.length) {
      return false
    }
    (nulls(idx) & MASKS(index % 8)) != 0
  }

  private def toRows(rs: TRowSet, delimiter: String): ListBuffer[String] = {
    val colCnt = rs.getColumnsSize
    val rowSize = getRowSize(rs)
    val rows = ListBuffer[StringBuilder]()
    val cols = rs.getColumns
    for (i <- 0 until rowSize) {
      val line = new StringBuilder()
      for (j <- 0 until colCnt) {
        val col = cols.get(j)
        val v = col.getFieldValue() match {
          case _: TStringColumn =>
            if (isNull(col.getStringVal.getNulls, i)) {
              NULL
            } else {
              var value = col.getStringVal.getValues.get(i)
              if (null != value) {
                if (value.contains("\"")) {
                  value = value.replace("\"", "\"\"")
                }
                /*
                if (value.contains(delimiter)) {
                  "\"" + value + "\""
                } else {
                  value
                }
                 */
                "\"" + value + "\""
              } else {
                value
              }
            }
          case _: TDoubleColumn =>
            if (isNull(col.getDoubleVal.getNulls, i)) NULL else col.getDoubleVal.getValues.get(i)
          case _: TI64Column =>
            if (isNull(col.getI64Val.getNulls, i)) NULL else col.getI64Val.getValues.get(i)
          case _: TI32Column =>
            if (isNull(col.getI32Val.getNulls, i)) NULL else col.getI32Val.getValues.get(i)
          case _: TI16Column =>
            if (isNull(col.getI16Val.getNulls, i)) NULL else col.getI16Val.getValues.get(i)
          case _: TBoolColumn =>
            if (isNull(col.getBoolVal.getNulls, i)) NULL else col.getBoolVal.getValues.get(i)
          case _: TByteColumn =>
            if (isNull(col.getByteVal.getNulls, i)) NULL else col.getByteVal.getValues.get(i)
          case _: TBinaryColumn =>
            if (isNull(col.getBinaryVal.getNulls, i)) NULL else col.getBinaryVal.getValues.get(i)
          case _ => s"UNSUPPORTED_TYPE:${col.getFieldValue()}"
        }
        line.append(v).append(delimiter)
      }
      rows.append(line)
    }

    debug(s"rows: $rows")
    rows.map { a =>
      if (a.endsWith(delimiter)) {
        a.substring(0, a.length - 1)
      } else {
        a.toString()
      }
    }
  }

  private def needAuth(conf: KyuubiConf): Boolean = {
    val engineType = conf.get(ENGINE_TYPE)
    val provider = conf.get(ENGINE_JDBC_CONNECTION_PROVIDER)
    SqlUtils.needAuth(
      conf.get(KYUUBI_AUTHORIZATION_ENABLED),
      engineType,
      provider.getOrElse(""))
  }

  private def authSql(statement: String, conf: KyuubiConf): SqlInfo = {
    val engineType = conf.get(ENGINE_TYPE)
    val provider = conf.get(ENGINE_JDBC_CONNECTION_PROVIDER).orNull
    val sm = be.sessionManager.asInstanceOf[KyuubiSessionManager]
    SqlUtils.authSql(
      sm.authorizationManager,
      statement,
      SqlUtils.parseEngineType(engineType, provider),
      conf)
  }

  private def pushSql(operation: Operation, sqlInfo: SqlInfo): Unit = {
    debug(s"PushSql ${operation.getClass}")
    operation match {
      case op: ExecuteStatement =>
        if (op.isPushedSql) {
          return
        }
        op.setPushedSql()
        val session = op.getSession.asInstanceOf[KyuubiSession]
        val engineType = session.sessionConf.get(ENGINE_TYPE)
        val provider = conf.get(ENGINE_JDBC_CONNECTION_PROVIDER).orNull
        SqlUtils.pushSql(
          session.sessionManager.asInstanceOf[KyuubiSessionManager].authorizationManager,
          op.statement,
          sqlInfo,
          SqlUtils.parseEngineType(engineType, provider),
          session.sessionConf)
      case _ =>
    }
  }

}
