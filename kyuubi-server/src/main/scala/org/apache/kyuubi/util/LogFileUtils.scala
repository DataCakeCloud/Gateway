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

package org.apache.kyuubi.util

import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging

object LogFileUtils extends Logging {

  private val logPaths = ArrayBuffer[Path]()

  def register(logPath: String): Unit = {
    if (StringUtils.isBlank(logPath)) {
      throw new IllegalArgumentException("logPath is empty")
    }
    register(Paths.get(logPath))
  }

  def register(logPath: Path): Unit = {
    if (null == logPath) {
      throw new IllegalArgumentException("logPath is null")
    }

    if (!logPaths.contains(logPath)) {
      logPaths += logPath
    }
  }

  def scan(timeout: Long): Unit = synchronized {
    val currentTime = System.currentTimeMillis()
    var fileCnt = 0
    var dirCnt = 0
    logPaths.foreach { path =>
      if (Files.exists(path)) {
        path.toFile.listFiles.filter(_.lastModified() < currentTime - timeout)
          .foreach { existsFile =>
            try {
              if (existsFile.isFile) {
                debug(s"DeleteFileName: ${existsFile.getName}")
                existsFile.delete()
                fileCnt += 1
              } else {
                FileUtils.deleteAll(existsFile)
                dirCnt += 1
              }
            } catch {
              case e: Exception =>
                warn(s"failed to delete engine log file: ${existsFile.getAbsolutePath}", e)
            }
          }
      }
    }
    val endTime = System.currentTimeMillis()
    info(s"DeleteLog file[$fileCnt] dir[$dirCnt] cost: [${endTime - currentTime} ms]")
  }
}
