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

import java.io.{BufferedWriter, File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.commons.io.FileUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.util.{AutoRetry, CSVUtil}

class StorageOperation(
    ss: StorageSession,
    val sessionId: String,
    val operationId: String,
    val output: String,
    val filename: String,
    val format: String,
    val delimiter: String,
    localParentPath: String,
    var maxFileSize: Long,
    os: ObjectStorage) extends Logging {

  private val UTF8_BOM = CSVUtil.HEADER_UTF8_BOM
  private val WRITE_DATA_TIMEOUT = 1800 * 1000L
  private val SINGLE_FILE_SIZE_LIMIT = 100 * 1024 * 1024
  private val MAX_PACKAGE_CNT = 10000
  private val dataQ = new LinkedBlockingQueue[List[String]](MAX_PACKAGE_CNT)
  private val closed = new AtomicBoolean(true)
  private val writtenHeader = new AtomicBoolean(false)

  private var bw: BufferedWriter = _
  private var currentFile: File = _
  private var currentFileIndex = 0
  private var lastWriteTime = 0L

  if (maxFileSize <= 0) {
    maxFileSize = SINGLE_FILE_SIZE_LIMIT
  }

  private val parser = ObjectStorageUrlParser.parser(output)
  parser.setPrefix(parser.getPrefix + sessionId + "/" + operationId + "/")

  private def genFilename(filename: String, format: String, index: Int): String = {
    if (index == 0) {
      filename + "." + format
    } else {
      filename + "_" + index + "." + format
    }
  }

  def open(): Task = synchronized {
    val parent = Paths.get(localParentPath, operationId)
    if (Files.notExists(parent)) {
      Files.createDirectories(parent)
    }
    val firstFile = Paths.get(parent.toString, genFilename(filename, format, currentFileIndex))
    currentFile = firstFile.toFile
    if (!currentFile.exists()) {
      currentFile.createNewFile()
    }
    bw = Files.newBufferedWriter(firstFile, StandardCharsets.UTF_8)
    bw.write(UTF8_BOM)
    closed.set(false)
    new Task
  }

  private def check(): Unit = {
    if (null == bw || isClosed) {
      throw new IOException("writer is null or closed")
    }
  }

  def isClosed: Boolean = closed.get()

  def close(force: Boolean): Unit = {
    info(s"Session[$sessionId] Op[$operationId] close force[$force]")
    if (isClosed) {
      return
    }
    closed.set(true)
    try {
      if (!force) {
        while (!dataQ.isEmpty) {
          info(s"Session[$sessionId] Op[$operationId] close but dataQ not empty wait 1s")
          Thread.sleep(1000L)
        }
      }
      if (null != bw) {
        bw.close()
        bw = null
      }
    } catch {
      case e: Exception =>
        error(s"Session[$sessionId] Op[$operationId] writer close failed and ignore", e)
    }
  }

  def commit(): Boolean = synchronized {
    info(s"Session[$sessionId] Op[$operationId] commit")
    try {
      close(false)
      upload()
    } catch {
      case e: Exception =>
        error(s"Session[$sessionId] Op[$operationId] upload file failed", e)
        // delete already upload file
        os.deletePath(parser.getBucketName, parser.getPrefix)
        return false
    } finally {
      shutdown()
    }
    true
  }

  def cancel(): Unit = synchronized {
    close(true)
  }

  private def upload(): Unit = {
    val parent = Paths.get(localParentPath, operationId)
    val p = parent.toFile
    val files = p.listFiles()
    if (!p.exists() || null == files) {
      error(s"Session[$sessionId] Op[$operationId] upload file to " +
        s"objectStorage failed reason object files empty")
      return
    }
    files.foreach { f =>
      info(s"Session[$sessionId] Op[$operationId] upload file[${f.getAbsolutePath}]")
      val objectKey = parser.getPrefix + f.getName
      try AutoRetry.executeWithRetry(
          () => {
            os.putObject(parser.getBucketName, objectKey, f)
            true
          },
          3,
          3000L,
          true)
      catch {
        case e: Exception =>
          error(
            s"Session[$sessionId] Op[$operationId] upload " +
              s"file to objectStorage failed: [$objectKey]",
            e)
          throw new Exception(e)
      }
    }
  }

  def clean(): Unit = {
    // clean local file
    val parent = Paths.get(localParentPath, operationId)
    info(s"Session[$sessionId] Op[$operationId] clean path[$parent]")
    try {
      FileUtils.deleteDirectory(parent.toFile)
    } catch {
      case e: Exception =>
        error(s"Clean path[$parent] failed", e)
    }
  }

  private def renewFile(): Unit = synchronized {
    check()
    bw.close()
    currentFileIndex += 1
    val path = Paths.get(
      currentFile.getParentFile.getAbsolutePath,
      genFilename(filename, format, currentFileIndex))
    val newFile = path.toFile
    info(s"Session[$sessionId] Op[$operationId] renewFile file[$path]")
    if (!newFile.exists() && !newFile.createNewFile()) {
      error(s"Session[$sessionId] Op[$operationId] renewFile failed, file[$path]")
      throw new IOException("renewFile failed")
    }
    bw = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
    bw.write(UTF8_BOM)
    currentFile = newFile
  }

  def writeHeader(header: String): Unit = {
    if (writtenHeader.get()) {
      return
    }
    writtenHeader.set(true)
    bw.write(header)
    bw.newLine()
  }

  def write(rows: List[String]): Unit = {
    if (null == rows || rows.isEmpty) {
      return
    }
    debug(s"Session[$sessionId] Op[$operationId] write rows.size[${rows.size}]")
    check()
    lastWriteTime = System.currentTimeMillis()
    try {
      while (!dataQ.offer(rows, 3, TimeUnit.SECONDS)) {
        warn(s"Session[$sessionId] Op[$operationId] write dataQ full, sleep 3s")
        Thread.sleep(3000L)
      }
    } catch {
      case _: InterruptedException =>
    }
  }

  private def writeFile(rows: List[String]): Unit = {
    if (null == rows || rows.isEmpty) {
      warn(s"Session[$sessionId] Op[$operationId] writeFile rows empty")
      return
    }
    info(s"Session[$sessionId] Op[$operationId] writeFile rows.size[${rows.size}]")
    if (currentFile.length() > maxFileSize) {
      renewFile()
    }
    try {
      rows.foreach { row =>
        bw.write(row)
        bw.newLine()
      }
    } finally {
      bw.flush()
    }
  }

  private def writeTimeout(): Boolean = {
    lastWriteTime > 0 && System.currentTimeMillis() - lastWriteTime > WRITE_DATA_TIMEOUT
  }

  def shutdown(): Unit = os.close()

  class Task extends Runnable {

    override def run(): Unit = {
      var rows: List[String] = null
      try {
        while (!isClosed || !dataQ.isEmpty) {
          rows = dataQ.poll(3, TimeUnit.SECONDS)
          if (null != rows) {
            writeFile(rows)
          } else {
            warn(s"Session[$sessionId] Op[$operationId] write dataQ empty, sleep 3s")
            Thread.sleep(3000L)
          }
          if (writeTimeout()) {
            warn(s"Session[$sessionId] Op[$operationId] write timeout")
            ss.updateState(OperationState.UPLOAD_TIMEOUT)
            throw new Exception("write data timeout")
          }
        }
      } catch {
        case e: Exception =>
          error(
            s"Session[$sessionId] Op[$operationId] writeFile failed so cancel handle",
            e)
          cancel()
      }
    }
  }
}
