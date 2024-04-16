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

import java.io.IOException
import java.nio.file.Paths
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.AutoRetry

class StorageSession(
    ksm: KyuubiSessionManager,
    val sessionId: String,
    output: String,
    filename: String,
    format: String,
    delimiter: String,
    fileMaxSize: Long,
    os: ObjectStorage,
    tp: ThreadPoolExecutor) extends Logging {

  private val KYUUBI_WORK_DIR_ROOT = sys.env.get("KYUUBI_WORK_DIR_ROOT").orNull
  private val STORAGE_PATH = if (StringUtils.isNotBlank(KYUUBI_WORK_DIR_ROOT)) {
    Paths.get(KYUUBI_WORK_DIR_ROOT, "storage").toString
  } else {
    Paths.get("work", "storage").toString
  }

  val map = new ConcurrentHashMap[String, StorageOperation]()

  def openStorageOperation(operationId: String): StorageOperation = {
    if (map.contains(operationId)) {
      map.get(operationId)
    } else {
      val operation = new StorageOperation(
        this,
        sessionId,
        operationId,
        output,
        filename,
        format,
        delimiter,
        Paths.get(STORAGE_PATH, sessionId).toString,
        fileMaxSize,
        os)
      map.put(operationId, operation)
      tp.execute(operation.open())
      info(s"OpenStorageOperation current storageExecPoolSize[${tp.getPoolSize}], " +
        s"storageExecPoolActiveCount[${tp.getActiveCount}]")
      operation
    }
  }

  def getOperation(operationId: String): StorageOperation = map.get(operationId)

  def removeOperation(operationId: String): Unit = {
    val op = map.remove(operationId)
    if (null != op) {
      op.cancel()
      op.clean()
    }
  }

  def commit(): Unit = {
    map.values().forEach { op =>
      op.commit()
    }
    updateState(OperationState.UPLOADED)
    clean()
  }

  def cancel(): Unit = {
    map.values().forEach { op =>
      op.cancel()
      op.shutdown()
    }
  }

  def clean(): Unit = {
    map.values().forEach { op =>
      op.clean()
    }
    map.clear()
    try {
      FileUtils.deleteDirectory(Paths.get(STORAGE_PATH, sessionId).toFile)
    } catch {
      case e: IOException =>
        error(s"Session[$sessionId] clean failed and ignore", e)
    }
  }

  def updateState(state: OperationState): Boolean = {
    val endTime = System.currentTimeMillis()
    try {
      AutoRetry.executeWithRetry(
        () => {
          ksm.updateMetadata(Metadata(
            identifier = sessionId,
            state = state.toString,
            endTime = endTime))
        },
        3,
        3000L,
        true)
    } catch {
      case e: Exception =>
        error(s"Session[$sessionId] update state [$state] failed", e);
    }
    false
  }
}
