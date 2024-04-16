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

package org.apache.kyuubi.session.jdbc

import java.sql.{ResultSet, Statement}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kyuubi.operation.{KyuubiOperation, OperationState}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class KyuubiJDBCOperation(
    session: Session,
    override val statement: String,
    confOverlay: Map[String, String],
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    incrementalCollect: Boolean = false)
  extends KyuubiOperation(session) {

  // protected[operation] lazy val connection = session.asInstanceOf[KyuubiJdbcSessionImpl]
  // .connection

  private var stmt: Statement = _
  private var rs: ResultSet = _

  final private val _operationLog: OperationLog =
    if (shouldRunAsync) {
      OperationLog.createOperationLog(session, getHandle)
    } else {
      null
    }

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  private val pushedSql: AtomicBoolean = new AtomicBoolean(false)

  def setPushedSql(): Unit = pushedSql.set(true)

  def isPushedSql: Boolean = pushedSql.get

  override def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setState(OperationState.PENDING)
    sendCredentialsIfNeeded()
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  private def executeStatement(): Unit = {
//    try {
//      stmt = connection.createStatement()
//      if (shouldRunAsync) {
//        val hasResult = stmt.execute(statement)
//        setHasResultSet(hasResult)
//      } else {
//        rs = stmt.executeQuery(statement)
//        setHasResultSet(true)
//      }
//    } catch onError()
  }

  private def waitStatementComplete(): Unit = {
    try {
      setState(OperationState.RUNNING)
      rs = stmt.getResultSet
    } finally {}
  }

  override protected def runInternal(): Unit = {
    executeStatement()
    val sessionManager = session.sessionManager
    val asyncOperation: Runnable = () => waitStatementComplete()
    try {
      val opHandle = sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch onError("submitting query in background, query rejected")

    if (!shouldRunAsync) getBackgroundHandle.get()
  }

  override protected def eventEnabled: Boolean = true
}
