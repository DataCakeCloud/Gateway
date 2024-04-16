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

package org.apache.kyuubi.storage

import java.util.concurrent._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.NamedThreadFactory

class StorageManager(
    ksm: KyuubiSessionManager,
    corePoolSize: Int = 0,
    maxPoolSize: Int = 0,
    waitQueueSize: Int = 0,
    keepAliveTimeMillis: Long = 0L) extends Logging {

  val map = new ConcurrentHashMap[String, StorageSession]()
  val tp: ThreadPoolExecutor = new ThreadPoolExecutor(
    corePoolSize,
    maxPoolSize,
    keepAliveTimeMillis,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable](waitQueueSize),
    new NamedThreadFactory("StorageSession", daemon = true))

  tp.allowCoreThreadTimeOut(true)

  def newStorageSession(
      sessionId: String,
      output: String,
      filename: String,
      format: String,
      delimiter: String,
      fileMaxSize: Long,
      os: ObjectStorage): StorageSession = synchronized {
    return map.computeIfAbsent(
      sessionId,
      _ => {
        new StorageSession(ksm, sessionId, output, filename, format, delimiter, fileMaxSize, os, tp)
      })
  }

  def findOperation(operationId: String): StorageOperation = {
    map.values().forEach { session =>
      val op = session.getOperation(operationId)
      if (null != op) {
        return op
      }
    }
    null
  }

  def findSessionByOperation(operationId: String): StorageSession = {
    map.values().forEach { session =>
      val op = session.getOperation(operationId)
      if (null != op) {
        return session
      }
    }
    null
  }

  def getSession(sessionId: String): StorageSession = {
    map.get(sessionId)
  }

  def cancelSession(sessionId: String): Unit = {
    val session = getSession(sessionId)
    if (null != session) {
      session.cancel()
    }
  }

  def closeSession(sessionId: String): Unit = {
    info(s"CloseSession session[$sessionId]")
    val session = getSession(sessionId)
    if (null != session) {
      tp.execute(() => {
        info(s"Session[$sessionId] commit")
        if (null != map.remove(sessionId)) {
          session.commit()
        }
      })
    }
  }

  def getExecPoolSize: Int = tp.getPoolSize

  def getActiveCount: Int = tp.getActiveCount

  def getWaitQueueSize: Int = {
    val q = tp.getQueue
    if (null == q) {
      return 0
    }
    q.size()
  }

  def shutdown(timeoutMillis: Long): Unit = {
    map.values().forEach(_.commit())
    tp.shutdown()
    try {
      tp.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: Exception =>
        warn(s"Exceeded timeout($timeoutMillis ms) to wait the exec-pool shutdown gracefully", e)
    }
  }
}
