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

import java.util.concurrent.{Future, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiConf.OPERATION_IDLE_TIMEOUT
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState._
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThreadUtils

abstract class AbstractOperation(session: Session) extends Operation with Logging {

  final protected val opType: String = getClass.getSimpleName
  final protected val createTime = System.currentTimeMillis()
  final private val handle = OperationHandle()
  final private val operationTimeout: Long = {
    session.sessionManager.getConf.get(OPERATION_IDLE_TIMEOUT)
  }

  final private[kyuubi] val statementId = handle.identifier.toString

  private var statementTimeoutCleaner: Option[ScheduledExecutorService] = None

  protected def cleanup(targetState: OperationState): Unit = state.synchronized {
    if (!isTerminalState(state)) {
      setState(targetState)
      Option(getBackgroundHandle).foreach(_.cancel(true))
    }
  }

  protected def addTimeoutMonitor(queryTimeout: Long): Unit = {
    if (queryTimeout > 0) {
      val timeoutExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("query-timeout-thread", false)
      val action: Runnable = () => cleanup(OperationState.TIMEOUT)
      timeoutExecutor.schedule(action, queryTimeout, TimeUnit.SECONDS)
      statementTimeoutCleaner = Some(timeoutExecutor)
    }
  }

  protected def shutdownTimeoutMonitor(): Unit = {
    statementTimeoutCleaner.foreach(_.shutdown())
  }

  override def getOperationLog: Option[OperationLog] = None

  @volatile protected var state: OperationState = INITIALIZED
  @volatile protected var startTime: Long = _
  @volatile protected var completedTime: Long = _
  @volatile protected var lastAccessTime: Long = createTime

  @volatile protected var operationException: KyuubiSQLException = _
  @volatile protected var operationJobProgress: TProgressUpdateResp = _

  @volatile protected var hasResultSet: Boolean = false

  @volatile private var _backgroundHandle: Future[_] = _

  protected def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    _backgroundHandle = backgroundHandle
  }

  def getBackgroundHandle: Future[_] = _backgroundHandle

  def statement: String = opType

  def redactedStatement: String = statement

  protected def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
    handle.setHasResultSet(hasResultSet)
  }

  protected def setOperationException(opEx: KyuubiSQLException): Unit = {
    this.operationException = opEx
  }

  def setOperationJobProgress(opJobProgress: TProgressUpdateResp): Unit = {
    this.operationJobProgress = opJobProgress
  }

  protected def setState(newState: OperationState): Unit = {
    OperationState.validateTransition(state, newState)
    newState match {
      case RUNNING =>
        info(s"Processing ${session.user}'s query[$statementId]: " +
          s"${state.name} -> ${newState.name}, statement:\n$redactedStatement")
        startTime = System.currentTimeMillis()
      case ERROR | FINISHED | CANCELED | TIMEOUT =>
        completedTime = System.currentTimeMillis()
        val timeCost = s", time taken: ${(completedTime - startTime) / 1000.0} seconds"
        info(s"Processing ${session.user}'s query[$statementId]: " +
          s"${state.name} -> ${newState.name}$timeCost")
      case _ =>
    }
    state = newState
    lastAccessTime = System.currentTimeMillis()
  }

  protected def isClosedOrCanceled: Boolean = {
    state == OperationState.CLOSED || state == OperationState.CANCELED
  }

  protected def isTerminalState(operationState: OperationState): Boolean = {
    OperationState.isTerminal(operationState)
  }

  protected def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new IllegalStateException(s"Expected state $state, but found ${this.state}")
    }
    lastAccessTime = System.currentTimeMillis()
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   */
  protected def validateDefaultFetchOrientation(orientation: FetchOrientation): Unit = {
    validateFetchOrientation(orientation, Operation.DEFAULT_FETCH_ORIENTATION_SET)
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   */
  private def validateFetchOrientation(
      orientation: FetchOrientation,
      supportedOrientations: Set[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw KyuubiSQLException(s"The fetch type $orientation is not supported for this ResultSet.")
    }
  }

  protected def runInternal(): Unit

  protected def beforeRun(): Unit

  protected def afterRun(): Unit

  override def run(): Unit = {
    beforeRun()
    try {
      runInternal()
    } finally {
      afterRun()
    }
  }

  override def cancel(): Unit

  override def close(): Unit

  protected def getProtocolVersion: TProtocolVersion = session.protocol

  override def getResultSetMetadata: TGetResultSetMetadataResp

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet

  /**
   * convert SQL 'like' pattern to a Java regular expression.
   *
   * Underscores (_) are converted to '.' and percent signs (%) are converted to '.*'.
   *
   * (referred to Spark's implementation: convertPattern function in file MetadataOperation.java)
   *
   * @param input the SQL pattern to convert
   * @return the equivalent Java regular expression of the pattern
   */
  protected def toJavaRegex(input: String): String = {
    val res =
      if (StringUtils.isEmpty(input) || input == "*") {
        "%"
      } else {
        input
      }
    val wStr = ".*"
    res
      .replaceAll("([^\\\\])%", "$1" + wStr).replaceAll("\\\\%", "%").replaceAll("^%", wStr)
      .replaceAll("([^\\\\])_", "$1.").replaceAll("\\\\_", "_").replaceAll("^_", ".")
  }

  override def getSession: Session = session

  override def getHandle: OperationHandle = handle

  override def getQueryId: String = {
    null
  }

  override def getStatus: OperationStatus = {
    lastAccessTime = System.currentTimeMillis()
    OperationStatus(
      state,
      createTime,
      startTime,
      lastAccessTime,
      completedTime,
      hasResultSet,
      Option(operationException),
      Option(operationJobProgress))
  }

  override def shouldRunAsync: Boolean

  override def isTimedOut: Boolean = {
    if (operationTimeout <= 0) {
      false
    } else {
      OperationState.isTerminal(state) &&
      lastAccessTime + operationTimeout <= System.currentTimeMillis()
    }
  }

  final val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)
  final val ERROR_STATUS = new TStatus(TStatusCode.ERROR_STATUS)

  def okStatusWithHints(hints: Seq[String]): TStatus = {
    val ok = new TStatus(TStatusCode.SUCCESS_STATUS)
    ok.setInfoMessages(hints.asJava)
    ok
  }
}
