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

package org.apache.kyuubi.engine

import java.io.{File, FilenameFilter, IOException}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.EvictingQueue
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.containsIgnoreCase

import org.apache.kyuubi._
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.storage.{ObjectStorage, ObjectStorageFactory, ObjectStorageType, ObjectStorageUrlParser}
import org.apache.kyuubi.util.{DateUtil, FileUtils, NamedThreadFactory}

trait ProcBuilder {

  import ProcBuilder._

  /**
   * The short name of the engine process builder, we use this for form the engine jar paths now
   * see `mainResource`
   */
  def shortName: String

  /**
   * executable, it is `JAVA_HOME/bin/java` by default
   */
  protected def executable: String = {
    val javaHome = env.get("JAVA_HOME")
    if (javaHome.isEmpty) {
      throw validateEnv("JAVA_HOME")
    } else {
      Paths.get(javaHome.get, "bin", "java").toString
    }
  }

  /**
   * The engine jar or other runnable jar containing the main method
   */
  def mainResource: Option[String] = {
    // 1. get the main resource jar for user specified config first
    // TODO use SPARK_SCALA_VERSION instead of SCALA_COMPILE_VERSION
    val jarName = s"${module}_$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"
    conf.getOption(s"kyuubi.session.engine.$shortName.main.resource").filter { userSpecified =>
      // skip check exist if not local file.
      val uri = new URI(userSpecified)
      val schema = if (uri.getScheme != null) uri.getScheme else "file"
      schema match {
        case "file" => Files.exists(Paths.get(userSpecified))
        case _ => true
      }
    }.orElse {
      // 2. get the main resource jar from system build default
      env.get(KYUUBI_HOME).toSeq
        .flatMap { p =>
          Seq(
            Paths.get(p, "externals", "engines", shortName, jarName),
            Paths.get(p, "externals", module, "target", jarName))
        }
        .find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse {
      // 3. get the main resource from dev environment
      val cwd = Utils.getCodeSourceLocation(getClass).split("kyuubi-server")
      assert(cwd.length > 1)
      Option(Paths.get(cwd.head, "externals", module, "target", jarName))
        .map(_.toAbsolutePath.toFile.getCanonicalPath)
    }
  }

  protected def module: String

  /**
   * The class containing the main method
   */
  protected def mainClass: String

  protected def proxyUser: String

  protected def engineRefId: String

  protected val commands: Array[String]

  def conf: KyuubiConf

  def env: Map[String, String] = conf.getEnvs

  protected val extraEngineLog: Option[OperationLog]

  /**
   * Add `engine.master` if KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT
   * are defined. So we can deploy engine on kubernetes without setting `engine.master`
   * explicitly when kyuubi-servers are on kubernetes, which also helps in case that
   * api-server is not exposed to us.
   */
  protected def completeMasterUrl(conf: KyuubiConf): Unit = {}

  protected[kyuubi] def clean(): Unit = {}

  protected val workingDir: Path = {
    env.get("KYUUBI_WORK_DIR_ROOT").map { root =>
      val workingRoot = Paths.get(root).toAbsolutePath
      if (!Files.exists(workingRoot)) {
        info(s"Creating KYUUBI_WORK_DIR_ROOT at $workingRoot")
        Files.createDirectories(workingRoot)
      }
      if (Files.isDirectory(workingRoot)) {
        workingRoot.toString
      } else null
    }.map { rootAbs =>
      val working = Paths.get(rootAbs, ENGINE_LOG_PREFIX)
      if (!Files.exists(working)) {
        info(s"Creating $ENGINE_LOG_PREFIX's working directory at $working")
        Files.createDirectories(working)
      }
      if (Files.isDirectory(working)) {
        working
      } else {
        Utils.createTempDir(ENGINE_LOG_PREFIX, rootAbs)
      }
    }.getOrElse {
      Utils.createTempDir(prefix = ENGINE_LOG_PREFIX)
    }
  }

  final lazy val processBuilder: ProcessBuilder = {
    val pb = new ProcessBuilder(commands: _*)

    val envs = pb.environment()
    envs.putAll(env.asJava)
    pb.directory(workingDir.toFile)
    redirectOutput(pb)
    redirectError(pb)
    extraEngineLog.foreach(_.addExtraLog(engineLog.toPath))
    extraEngineLog.foreach(_.addExtraLog(remoteLog.toPath))
    pb
  }

  def redirectOutput(pb: ProcessBuilder): Unit = pb.redirectOutput(engineLog)

  def redirectError(pb: ProcessBuilder): Unit = pb.redirectError(engineLog)

  def setProcAuthEnv(cluster: Cluster): ProcBuilder = {
    conf.get(AUTHENTICATION_METHOD).head match {
      case "KERBEROS" =>
        setProcEnv("KRB5CCNAME", conf.get(KYUUBI_KINIT_PATH))
        if (null != cluster) {
          setProcEnv("HADOOP_CONF_DIR", cluster.getYarnClusterConfigPath)
        }
      case _ =>
    }
    this
  }

  def setProcEnv(): ProcBuilder = {
    this
  }

  def setProcEnv(key: String, value: String): ProcBuilder = {
    if (null != processBuilder) {
      processBuilder.environment().put(key, value)
    }
    this
  }

  def getProcEnvs: mutable.Map[String, String] = {
    if (null == processBuilder) {
      return mutable.Map.empty[String, String]
    }
    processBuilder.environment().asScala
  }

  def getProcEnv(key: String): String = {
    if (null == processBuilder) {
      return StringUtils.EMPTY
    }
    processBuilder.environment().get(key)
  }

  @volatile private var error: Throwable = UNCAUGHT_ERROR

  private val engineLogMaxLines = conf.get(KyuubiConf.SESSION_ENGINE_STARTUP_MAX_LOG_LINES)
  private val waitCompletion = conf.get(KyuubiConf.SESSION_ENGINE_STARTUP_WAIT_COMPLETION)
  protected val lastRowsOfLog: EvictingQueue[String] = EvictingQueue.create(engineLogMaxLines)
  // Visible for test
  @volatile private[kyuubi] var logCaptureThreadReleased: Boolean = true
  private var logCaptureThread: Thread = _
  private var process: Process = _
  @VisibleForTesting
  @volatile private[kyuubi] var processLaunched: Boolean = _

  private[kyuubi] lazy val engineLog: File = ProcBuilder.synchronized {
    val engineLogTimeout = conf.get(KyuubiConf.ENGINE_LOG_TIMEOUT)
    val currentTime = System.currentTimeMillis()
    val processLogPath = workingDir
    /*
    val totalExistsFile = processLogPath.toFile.listFiles { (_, name) => name.startsWith(module) }
    val sorted = totalExistsFile.sortBy(_.getName.split("\\.").last.toInt)
    val nextIndex =
      if (sorted.isEmpty) {
        0
      } else {
        sorted.last.getName.split("\\.").last.toInt + 1
      }
    val file = sorted.find(_.lastModified() < currentTime - engineLogTimeout)
      .map { existsFile =>
        try {
          // Here we want to overwrite the exists log file
          existsFile.delete()
          existsFile.createNewFile()
          existsFile
        } catch {
          case e: Exception =>
            warn(s"failed to delete engine log file: ${existsFile.getAbsolutePath}", e)
            null
        }
      }
      .getOrElse {
        Files.createDirectories(processLogPath)
        val newLogFile = new File(processLogPath.toFile, s"$module.log.$nextIndex")
        newLogFile.createNewFile()
        newLogFile
      }
     */

    // not scan timeout file and not delete at now
    /*
    processLogPath.toFile.listFiles.filter(_.lastModified() < currentTime - engineLogTimeout)
      .foreach { existsFile =>
        try {
          existsFile.delete()
        } catch {
          case e: Exception =>
            warn(s"failed to delete engine log file: ${existsFile.getAbsolutePath}", e)
        }
      }
     */
    val file = new File(processLogPath.toFile, s"$engineRefId")
    file.createNewFile()
    file.setLastModified(currentTime)
    info(s"Engine Logging to $file")
    file
  }

  private[kyuubi] lazy val remoteLog: File = synchronized {
    val file = new File(workingDir.toFile, s"$engineRefId$REMOTE_LOG_SUFFIX")
    file.createNewFile()
    file.setLastModified(System.currentTimeMillis())
    info(s"Remote Logging to $file")
    file
  }

  private[kyuubi] lazy val mergeLog: File = synchronized {
    val file = new File(workingDir.toFile, s"$engineRefId$MERGE_LOG_SUFFIX")
    file.createNewFile()
    file.setLastModified(System.currentTimeMillis())
    info(s"Merge Logging to $file")
    file
  }

  def validateConf(): Unit = {}

  final def start: Process = synchronized {
    process = processBuilder.start()
    processLaunched = true
    val reader = Files.newBufferedReader(engineLog.toPath, StandardCharsets.UTF_8)

    val redirect: Runnable = { () =>
      try {
        val maxErrorSize = conf.get(KyuubiConf.ENGINE_ERROR_MAX_SIZE)
        while (true) {
          if (reader.ready()) {
            var line: String = reader.readLine()
            if (containsException(line) &&
              !line.contains("at ") && !line.startsWith("Caused by:")) {
              val sb = new StringBuilder(line)
              error = KyuubiSQLException(sb.toString() + s"\n See more: $engineLog")
              line = reader.readLine()
              while (sb.length < maxErrorSize && line != null &&
                (containsException(line) ||
                  line.startsWith("\tat ") ||
                  line.startsWith("Caused by: "))) {
                sb.append("\n" + line)
                line = reader.readLine()
              }

              error = KyuubiSQLException(sb.toString() + s"\n See more: $engineLog")
            } else if (line != null) {
              lastRowsOfLog.add(line)
            }
          } else {
            Thread.sleep(300)
          }
        }
      } catch {
        case _: IOException =>
        case _: InterruptedException =>
      } finally {
        logCaptureThreadReleased = true
        reader.close()
      }
    }

    logCaptureThreadReleased = false
    logCaptureThread = PROC_BUILD_LOGGER.newThread(redirect)
    logCaptureThread.start()
    process
  }

  def close(destroyProcess: Boolean = !waitCompletion): Unit = synchronized {
    if (logCaptureThread != null) {
      logCaptureThread.interrupt()
      logCaptureThread = null
    }
    if (destroyProcess && process != null) {
      info("Destroy the process, since waitCompletion is false.")
      process.destroyForcibly()
      process = null
    }
  }

  def getError: Throwable = synchronized {
    if (error == UNCAUGHT_ERROR) {
      Thread.sleep(1000)
    }
    val lastLogRows = lastRowsOfLog.toArray.mkString("\n")
    error match {
      case UNCAUGHT_ERROR =>
        KyuubiSQLException(s"Failed to detect the root cause, please check $engineLog at server " +
          s"side if necessary. The last $engineLogMaxLines line(s) of log are:\n" +
          s"${lastRowsOfLog.toArray.mkString("\n")}")
      case other =>
        KyuubiSQLException(s"${Utils.stringifyException(other)}.\n" +
          s"FYI: The last $engineLogMaxLines line(s) of log are:\n$lastLogRows")
    }
  }

  private def containsException(log: String): Boolean =
    containsIgnoreCase(log, "Exception:") || containsIgnoreCase(log, "Exception in thread")

  override def toString: String = {
    if (commands == null) {
      super.toString
    } else {
      Utils.redactCommandLineArgs(conf, commands).map {
        case arg if arg.startsWith("--") => s"\\\n\t$arg"
        case arg => arg
      }.mkString(" ")
    }
  }

  /**
   * Get the home directly that contains binary distributions of engines.
   *
   * Take Spark as an example, we first lookup the SPARK_HOME from user specified environments.
   * If not found, we assume that it is a dev environment and lookup the kyuubi-download's output
   * directly. If not found again, a `KyuubiSQLException` will be raised.
   * In summarize, we walk through
   *   `kyuubi.engineEnv.SPARK_HOME` ->
   *   System.env("SPARK_HOME") ->
   *   kyuubi-download/target/spark-* ->
   *   error.
   *
   * @param shortName the short name of engine, e.g. spark
   * @return SPARK_HOME, HIVE_HOME, etc.
   */
  protected def getEngineHome(shortName: String): String = {
    val homeDirFilter: FilenameFilter = (dir: File, name: String) =>
      dir.isDirectory && name.contains(s"$shortName-") && !name.contains("-engine")

    val homeKey = s"${shortName.toUpperCase}_HOME"
    // 1. get from env, e.g. SPARK_HOME, FLINK_HOME
    env.get(homeKey)
      .orElse {
        // 2. get from $KYUUBI_HOME/externals/kyuubi-download/target
        env.get(KYUUBI_HOME).flatMap { p =>
          val candidates = Paths.get(p, "externals", "kyuubi-download", "target")
            .toFile.listFiles(homeDirFilter)
          if (candidates == null) None else candidates.map(_.toPath).headOption
        }.filter(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
      }.orElse {
        // 3. get from kyuubi-server/../externals/kyuubi-download/target
        Utils.getCodeSourceLocation(getClass).split("kyuubi-server").flatMap { cwd =>
          val candidates = Paths.get(cwd, "externals", "kyuubi-download", "target")
            .toFile.listFiles(homeDirFilter)
          if (candidates == null) None else candidates.map(_.toPath).headOption
        }.find(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
      } match {
      case Some(homeVal) => homeVal
      case None => throw validateEnv(homeKey)
    }
  }

  protected[kyuubi] def validateEnv(requiredEnv: String): Throwable = {
    KyuubiSQLException(s"$requiredEnv is not set! For more information on installing and " +
      s"configuring $requiredEnv, please visit https://kyuubi.readthedocs.io/en/master/" +
      s"deployment/settings.html#environments")
  }

  def clusterManager(): Option[String] = None

  def cluster(): Option[Cluster] = None

}

object ProcBuilder extends Logging {
  val HADOOP_HOME = "HADOOP_HOME"
  val HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
  val HADOOP_CLASSPATH = "HADOOP_CLASSPATH"
  val YARN_CONF_DIR = "YARN_CONF_DIR"

  private val PROC_BUILD_LOGGER = new NamedThreadFactory("process-logger-capture", daemon = true)

  private val UNCAUGHT_ERROR = new RuntimeException("Uncaught error")

  private val ENGINE_LOG_PREFIX = "engine_log"
  private val REMOTE_LOG_SUFFIX = ".remote"
  private val MERGE_LOG_SUFFIX = ".merge"

  def findEngineLog(opLog: OperationLog, engineRefId: String): (File, File) = {
    val enginePath = Option(System.getenv("KYUUBI_WORK_DIR_ROOT")).map {
      root => Paths.get(root, ENGINE_LOG_PREFIX)
    }.getOrElse {
      Option(System.getenv("KYUUBI_HOME")).map {
        root =>
          Paths.get(root, "worker", ENGINE_LOG_PREFIX)
      }.getOrElse {
        Utils.createTempDir(prefix = ENGINE_LOG_PREFIX)
      }
    }
    if (!Files.exists(enginePath)) {
      Files.createDirectories(enginePath)
    }
    val engineLog = Paths.get(enginePath.toString, engineRefId).toFile
    if (!engineLog.exists()) {
      engineLog.createNewFile()
    }
    opLog.addExtraLog(engineLog.toPath)

    val remoteLog = Paths.get(enginePath.toString, s"$engineRefId$REMOTE_LOG_SUFFIX").toFile
    if (!remoteLog.exists()) {
      remoteLog.createNewFile()
    }
    opLog.addExtraLog(remoteLog.toPath)
    (engineLog, remoteLog)
  }

  def createSessionTmpPath(sessionId: String): String = {
    val path = Paths.get(Cluster.TEMP_PATH, sessionId)
    if (Files.notExists(path)) {
      Files.createDirectories(path)
    }
    path.toString
  }

  def mergeLogAndUpload(
      logId: String,
      createTime: Long,
      opLog: OperationLog,
      mergeLogPath: String,
      conf: KyuubiConf): Unit = {
    info(s"MergeLogAndUpload logId[$logId], mergeLog: [$mergeLogPath]")
    if (null == opLog) {
      error(s"MergeLogAndUpload logId[$logId] failed, opLog is null")
      return
    }
    findEngineLog(opLog, logId)
    try {
      val f = FileUtils.merge(opLog.allLogsPath().asJava, mergeLogPath)

      var uploadPath = conf.get(KYUUBI_ENGINE_LOG_UPLOAD_PREFIX)

      if (!uploadPath.endsWith("/")) {
        uploadPath += "/"
      }
      var ct = createTime
      if (ct <= 0) {
        ct = System.currentTimeMillis()
      }
      val date = DateUtil.timestampToDateByShort(ct)
      val parser = ObjectStorageUrlParser.parser(uploadPath + date)

      var storage: ObjectStorage = null
      parser.getStorageType match {
        case ObjectStorageType.S3 =>
          val uploadRegion = conf.get(KYUUBI_S3_REGION)
          storage = ObjectStorageFactory.create(parser.getStorageType, uploadRegion)
        case ObjectStorageType.GS =>
          storage = ObjectStorageFactory.create(parser.getStorageType, null)
        case ObjectStorageType.OBS =>
          val endpoint = conf.get(KYUUBI_OBS_ENDPOINT)
          val accessKey = conf.get(KYUUBI_OBS_ACCESS_KEY)
          val secretKey = conf.get(KYUUBI_OBS_SECRET_KEY)
          storage =
            ObjectStorageFactory.create(parser.getStorageType, endpoint, accessKey, secretKey)
        case ObjectStorageType.KS3 =>
          val endpoint = conf.get(KYUUBI_KS3_ENDPOINT)
          val accessKey = conf.get(KYUUBI_KS3_ACCESS_KEY)
          val secretKey = conf.get(KYUUBI_KS3_SECRET_KEY)
          storage =
            ObjectStorageFactory.create(parser.getStorageType, endpoint, accessKey, secretKey)
        case _ =>
      }
      if (null != storage) {
        val objectKey = s"${parser.getPrefix}/$logId.txt"
        info(s"MergeLogAndUpload logId[$logId], uploadPath: [$objectKey]")
        storage.putObject(parser.getBucketName, objectKey, f, "text/plain")
      }
    } catch {
      case e: Exception =>
        error(s"MergeLogAndUpload failed:", e)
    } finally {
      Files.deleteIfExists(Paths.get(mergeLogPath))
    }
  }
}
