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
package org.apache.kyuubi.engine.jdbc.operation

import java.nio.ByteBuffer
import java.sql.{Connection, Statement, Types}
import java.util

import org.apache.hive.service.rpc.thrift.{TColumn, TRow, TRowSet, TStringColumn}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.jdbc.schema.{Column, Row, Schema}
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.engine.jdbc.util.ResultSetWrapper
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.operation.{ArrayFetchIterator, IterableFetchIterator, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean)
  extends JdbcOperation(session) with Logging {

  var jdbcStatement: Statement = null

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override def getQueryLog(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    if (null != jdbcStatement && jdbcStatement.isInstanceOf[KyuubiStatement]) {
      val kyuubiStatement = jdbcStatement.asInstanceOf[KyuubiStatement]
      debug(s"hasMoreLogs: ${kyuubiStatement.hasMoreLogs}")
      val logs = kyuubiStatement.getExecLog
      debug(s"logs: $logs")
      val tColumn = TColumn.stringVal(new TStringColumn(logs, ByteBuffer.allocate(0)))
      val tRow = new TRowSet(0, new util.ArrayList[TRow](logs.size()))
      tRow.addToColumns(tColumn)
      tRow
    } else {
      super.getQueryLog(order, rowSetSize)
    }
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          executeStatement()
        }
      }
      val jdbcSessionManager = session.sessionManager
      val backgroundHandle = jdbcSessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(backgroundHandle)
    } else {
      executeStatement()
    }
  }

  private def executeStatement(): Unit = {
    setState(OperationState.RUNNING)
    try {
      val connection: Connection = session.asInstanceOf[JdbcSessionImpl].sessionConnection
      jdbcStatement = dialect.createStatement(connection)
      val hasResult = jdbcStatement.execute(statement)
      if (hasResult) {
        val resultSetWrapper = new ResultSetWrapper(jdbcStatement)
        val metadata = resultSetWrapper.getMetadata()
        schema = Schema(metadata)
        iter =
          if (incrementalCollect) {
            info("Execute in incremental collect mode")
            new IterableFetchIterator(resultSetWrapper.toIterable)
          } else {
            warn(s"Execute in full collect mode")
            new ArrayFetchIterator(resultSetWrapper.toArray())
          }
      } else {
        schema = Schema(List[Column](
          Column(
            "result",
            "INT",
            Types.INTEGER,
            precision = 20,
            scale = 0,
            label = "result",
            displaySize = 20)))
        iter = new ArrayFetchIterator(Array[Row](
          Row(List(jdbcStatement.getUpdateCount))))
      }
      setState(OperationState.FINISHED)
    } catch {
      onError(true)
    } finally {
      if (jdbcStatement != null) {
        try {
          jdbcStatement.closeOnCompletion()
        } catch {
          case e: Exception => error(e)
        }
      }
      shutdownTimeoutMonitor()
    }
  }
}
