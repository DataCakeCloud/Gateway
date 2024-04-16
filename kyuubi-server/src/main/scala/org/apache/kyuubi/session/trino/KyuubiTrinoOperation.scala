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

import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean

import io.trino.client.Column
import org.apache.hive.service.rpc.thrift.{TGetResultSetMetadataResp, TRowSet}

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.operation.{ArrayFetchIterator, FetchIterator, KyuubiOperation, OperationState}
import org.apache.kyuubi.operation.FetchOrientation._
import org.apache.kyuubi.operation.OperationState.isTerminal
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.session.trino.schema.{RowSet, SchemaHelper}

class KyuubiTrinoOperation(
    session: Session,
    statement: String,
    confOverlay: Map[String, String],
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean = false)
  extends KyuubiOperation(session) {

  protected var schema: List[Column] = _

  protected var iter: FetchIterator[List[Any]] = _

  final private val _operationLog: OperationLog =
    if (shouldRunAsync) {
      OperationLog.createOperationLog(session, getHandle)
    } else {
      null
    }

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  private var trinoStatement: KyuubiTrinoStatement = _

  private val pushedSql: AtomicBoolean = new AtomicBoolean(false)

  def setPushedSql(): Unit = pushedSql.set(true)

  def isPushedSql: Boolean = pushedSql.get

  override def getQueryId: String = {
    if (null != trinoStatement) {
      val info = trinoStatement.getTrinoClient.currentStatusInfo()
      if (null != info) {
        return info.getId
      }
    }
    ""
  }

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val sessionId = session.handle.identifier.toString
    while (!isTerminal(state) && null == schema) {
      info(s"GetResultSetMetadata session[$sessionId] state: [$state] schema not ready")
      Thread.sleep(1000L)
    }
    if (null != schema) {
      val resp = new TGetResultSetMetadataResp
      val tTableSchema = SchemaHelper.toTTableSchema(schema)
      resp.setSchema(tTableSchema)
      resp.setStatus(OK_STATUS)
      resp
    } else {
      error(s"GetResultSetMetadata session[$sessionId] state: [$state] get schema failed")
      val status = trinoStatement.getTrinoClient.finalStatusInfo()
      if (null != status.getError) {
        throw KyuubiSQLException(s"Query failed: [${status.getId}]: ${status.getError.getMessage}")
      } else {
        throw KyuubiSQLException(s"Query failed: [${status.getId}]: getResultSetMetadata is null")
      }
    }
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    (order, incrementalCollect) match {
      case (FETCH_NEXT, _) => iter.fetchNext()
      case (FETCH_PRIOR, false) => iter.fetchPrior(rowSetSize)
      case (FETCH_FIRST, false) => iter.fetchAbsolute(0)
      case _ =>
        val mode = if (incrementalCollect) "incremental collect" else "full collect"
        throw KyuubiSQLException(s"Fetch orientation[$order] is not supported in $mode mode")
    }
    val taken = iter.take(rowSetSize)
    val resultRowSet = RowSet.toTRowSet(taken.toList, schema, getProtocolVersion)
    resultRowSet.setStartRowOffset(iter.getPosition)
    resultRowSet
  }

  override def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  def executeStatement(): Unit = {
    setState(OperationState.RUNNING)
    try {
      trinoStatement = session.asInstanceOf[KyuubiTrinoSessionImpl].createTrinoStatement(
        statement,
        getOperationLog)
      setHasResultSet(true)
      schema = trinoStatement.getColumns
      val resultSet = trinoStatement.execute()
      iter =
        if (incrementalCollect) {
          info("Execute in incremental collect mode")
          FetchIterator.fromIterator(resultSet)
        } else {
          info("Execute in full collect mode")
          // trinoStatement.execute return iterator is lazy,
          // call toArray to strict evaluation will pull all result data into memory
          new ArrayFetchIterator(resultSet.toArray)
        }
      setState(OperationState.FINISHED)
    } catch {
      // We should use Throwable instead of Exception since `java.lang.NoClassDefFoundError`
      // could be thrown.
      case e: Throwable =>
        if (trinoStatement.getTrinoClient.isRunning) {
          trinoStatement.getTrinoClient.cancelLeafStage()
        }
        state.synchronized {
          val errMsg = Utils.stringifyException(e)
          if (state == OperationState.TIMEOUT) {
            val ke = KyuubiSQLException(s"Timeout operating $opType: $errMsg")
            setOperationException(ke)
            throw ke
          } else if (isTerminalState(state)) {
            setOperationException(KyuubiSQLException(errMsg))
            warn(s"Ignore exception in terminal state with $statementId: $errMsg")
          } else {
            error(s"Error operating $opType: $errMsg", e)
            val ke = KyuubiSQLException(s"Error operating $opType: $errMsg", e)
            setOperationException(ke)
            setState(OperationState.ERROR)
            throw ke
          }
        }
    } finally {
      shutdownTimeoutMonitor()
    }
  }

  private def fetchQueryLog(): Unit = {}

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          OperationLog.setCurrentOperationLog(getOperationLog.get)
          executeStatement()
        }
      }

      try {
        val trinoSessionManager = session.sessionManager
        val backgroundHandle = trinoSessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke =
            KyuubiSQLException("Error submitting query in background, query rejected", rejected)
          setOperationException(ke)
          throw ke
      }
    } else {
      executeStatement()
    }
  }
}
