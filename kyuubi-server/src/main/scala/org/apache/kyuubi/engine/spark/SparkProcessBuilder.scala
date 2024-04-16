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

package org.apache.kyuubi.engine.spark

import java.io.{BufferedWriter, File, FileWriter, IOException}
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_METHOD, ENGINE_JDBC_CONNECTION_DATABASE, SERVER_KEYTAB, SERVER_PRINCIPAL}
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.engine.KubernetesApplicationOperation.{KUBERNETES_SERVICE_HOST, KUBERNETES_SERVICE_PORT}
import org.apache.kyuubi.engine.ProcBuilder.{HADOOP_CONF_DIR, YARN_CONF_DIR}
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.{AuthUtil, Validator}

class SparkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    override val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None,
    override val cluster: Option[Cluster] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, conf: KyuubiConf) {
    this(proxyUser, conf, "")
  }

  import SparkProcessBuilder._

  private[kyuubi] val sparkHome = getEngineHome(shortName)

  private[kyuubi] val sparkConf = cluster match {
    case Some(clu) =>
      Utils.getPropertiesFromFile(Some(new File(clu.getSparkConfigFilePath)))
    case _ => Map.empty[String, String]
  }

  private lazy val tmpSparkConfFilePath: File = {
    val tmpPath = ProcBuilder.createSessionTmpPath(engineRefId)
    Paths.get(tmpPath, SPARK_CONF_FILE_NAME).toFile
  }

  override protected val executable: String = {
    Paths.get(sparkHome, "bin", SPARK_SUBMIT_FILE).toFile.getCanonicalPath
  }

  private[kyuubi] def resetSparkHome(): SparkProcessBuilder = {
    setProcEnv(SPARK_HOME, sparkHome)
    this
  }

  override def setProcEnv(): SparkProcessBuilder = cluster match {
    case Some(clu) =>
      clu.getClusterType match {
        case "k8s" =>
          val sparkEnv = new ArrayBuffer[String]()
          val opts = getProcEnv(SPARK_SUBMIT_OPTS)
          if (StringUtils.isNotBlank(opts)) {
            sparkEnv += opts
          }
          sparkEnv += s"-Dkubeconfig=${clu.getKubeConfigFilePath}"
          setProcEnv(SPARK_SUBMIT_OPTS, String.join(" ", sparkEnv.asJava))
        case "yarn" =>
          setProcEnv(HADOOP_CONF_DIR, clu.getYarnClusterConfigPath)
          setProcEnv(YARN_CONF_DIR, clu.getYarnClusterConfigPath)

        case _ =>
      }

      val env = clu.getCommand.getEnv
      if (null != env && !env.isEmpty) {
        env.forEach(a => {
          val ss = a.split("=")
          if (ss.length >= 2) {
            setProcEnv(ss(0).trim, ss(1).trim)
          }
        })
      }
      this
    case _ => this
  }

  override def mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  /**
   * Add `spark.master` if KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT
   * are defined. So we can deploy spark on kubernetes without setting `spark.master`
   * explicitly when kyuubi-servers are on kubernetes, which also helps in case that
   * api-server is not exposed to us.
   */
  override protected def completeMasterUrl(conf: KyuubiConf): Unit = {
    try {
      (
        cluster,
        sys.env.get(KUBERNETES_SERVICE_HOST),
        sys.env.get(KUBERNETES_SERVICE_PORT)) match {
        case (None, Some(kubernetesServiceHost), Some(kubernetesServicePort)) =>
          // According to "https://kubernetes.io/docs/concepts/architecture/control-plane-
          // node-communication/#node-to-control-plane", the API server is configured to listen
          // for remote connections on a secure HTTPS port (typically 443), so we set https here.
          val masterURL = s"k8s://https://${kubernetesServiceHost}:${kubernetesServicePort}"
          conf.set(MASTER_KEY, masterURL)
        case (Some(clu), _, _) =>
          conf.set(MASTER_KEY, clu.getMaster)
        case _ =>
      }
    } catch {
      case e: Exception =>
        warn("Failed when setting up spark.master with kubernetes environment automatically.", e)
    }
  }

  /**
   * Converts kyuubi config key so that Spark could identify.
   * - If the key is start with `spark.`, keep it AS IS as it is a Spark Conf
   * - If the key is start with `hadoop.`, it will be prefixed with `spark.hadoop.`
   * - Otherwise, the key will be added a `spark.` prefix
   */
  protected def convertConfigKey(key: String): String = {
    if (key.startsWith("spark.")) {
      key
    } else if (key.startsWith("hadoop.")) {
      "spark.hadoop." + key
    } else {
      "spark." + key
    }
  }

  protected[kyuubi] def addClusterConf(buffer: ArrayBuffer[String]): Unit = cluster match {
    case Some(clu) =>
      val conf = clu.getCommand.getConf
      if (null != conf) {
        conf.asScala.foreach {
          a =>
            buffer += CONF
            buffer += a
        }
      }
      clu.getClusterType match {
        case "k8s" =>
          buffer += CONF
          buffer += SPARK_KUBERNETES_UPLOAD_KEY + "=" + sparkConf(SPARK_KUBERNETES_UPLOAD_KEY)
          addNamedConf(buffer)
        case _ =>
      }

      buffer += SPARK_CONFIG_FILE_KEY

      if (null != clu.getNeedAccount) {
        // 复制 sparkConf 文件，并替换 AKSK
        buffer += copySparkConfFileToTmp(clu)
      } else {
        buffer += clu.getSparkConfigFilePath
      }
    case None =>
  }

  protected[kyuubi] def addDatabase(buffer: ArrayBuffer[String]): Unit = {
    buffer += CONF
    buffer += s"$SPARK_SQL_DEFAULT_DB_KEY=${conf.get(ENGINE_JDBC_CONNECTION_DATABASE)}"
  }

  protected[kyuubi] def addNamedConf(buffer: ArrayBuffer[String]): Unit = {
    buffer += CONF
    buffer += SPARK_DRIVER_LABEL_PREFIX + JOB_ID_LABEL_KEY + "=" + engineRefId
    buffer += CONF
    buffer += SPARK_EXECUTOR_LABEL_PREFIX + JOB_ID_LABEL_KEY + "=" + engineRefId
  }

  protected[kyuubi] def addLakeLineageConf(buffer: ArrayBuffer[String]): Unit = {
    cluster match {
      case Some(clu) =>
        buffer += CONF
        buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_CLUSTER + "=" + clu.getId
      case _ =>
    }
    buffer += CONF
    buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_REAL_USER + "=" + proxyUser
    // TODO disable param
    // buffer += CONF
    // buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_USER_GROUP + "=" + ""
    buffer += CONF
    buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_TASK_ID + "=" + conf.getOption(
      LAKECAT_CATALOG_PROBE_TASK_ID).getOrElse("")
    buffer += CONF
    buffer += SPARK_HADOOP_CONF_PREFIX + LAKECAT_CATALOG_PROBE_FROM + "=" + conf.getOption(
      LAKECAT_CATALOG_PROBE_FROM).getOrElse("")

  }

  protected[kyuubi] def addAuthentication(buffer: ArrayBuffer[String]): Unit = {
    conf.get(AUTHENTICATION_METHOD).head match {
      case AuthUtil.AUTHENTICATION_METHOD_KERBEROS =>
        val principal = conf.get(SERVER_PRINCIPAL).orNull
        val keytab = conf.get(SERVER_KEYTAB).orNull
        buffer += CONF
        buffer += s"$PRINCIPAL=$principal"
        buffer += CONF
        buffer += s"$KEYTAB=$keytab"
      case _ =>
    }
  }

  override def mainResource: Option[String] = {
    if (sparkConf.contains(GATEWAY_SPARK_MAIN_RESOURCE_KEY)) {
      sparkConf.get(GATEWAY_SPARK_MAIN_RESOURCE_KEY)
    } else {
      super.mainResource
    }
  }

  override protected val commands: Array[String] = {
    // complete `spark.master` if absent on kubernetes
    completeMasterUrl(conf)

    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    // set conf front of user's to avoid overwritten by smy
    addClusterConf(buffer)

    // add lakecat lineage params
    addLakeLineageConf(buffer)

    addDatabase(buffer)

    addAuthentication(buffer)

    var allConf = conf.getAll

    // if enable sasl kerberos authentication for zookeeper, need to upload the server keytab file
    if (AuthTypes.withName(conf.get(HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE))
        == AuthTypes.KERBEROS) {
      allConf = allConf ++ zkAuthKeytabFileConf(allConf)
    }

    allConf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"${convertConfigKey(k)}=$v"
    }

    // if the keytab is specified, PROXY_USER is not supported
    tryKeytab() match {
      case None =>
        setSparkUserName(proxyUser, buffer)
      // buffer += PROXY_USER
      // buffer += proxyUser
      case Some(name) =>
        setSparkUserName(name, buffer)
    }

    mainResource.foreach { r => buffer += r }

    buffer.toArray
  }

  private[kyuubi] def copySparkConfFileToTmp(cluster: Cluster): String = {
    var bw: BufferedWriter = null
    try {
      bw = new BufferedWriter(new FileWriter(tmpSparkConfFilePath))
    } finally {
      bw.close()
    }
    tmpSparkConfFilePath.getAbsolutePath
  }

  override def clean(): Unit = cluster match {
    case Some(clu) =>
      if (clu.getConfigPath.startsWith(Cluster.TEMP_PATH)) {
        FileUtils.deleteDirectory(new File(clu.getConfigPath))
      }
      if (tmpSparkConfFilePath.exists()) {
        FileUtils.deleteDirectory(tmpSparkConfFilePath.getParentFile)
      }
    case None =>
  }

  override protected def module: String = "kyuubi-spark-sql-engine"

  private def tryKeytab(): Option[String] = {
    val principal = conf.getOption(PRINCIPAL)
    val keytab = conf.getOption(KEYTAB)
    if (principal.isEmpty || keytab.isEmpty) {
      None
    } else {
      try {
        val ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(principal.get, keytab.get)
        if (ugi.getShortUserName != proxyUser) {
          warn(s"The session proxy user: $proxyUser is not same with " +
            s"spark principal: ${ugi.getShortUserName}, so we can't support use keytab. " +
            s"Fallback to use proxy user.")
          None
        } else {
          Some(ugi.getShortUserName)
        }
      } catch {
        case e: IOException =>
          error(s"Failed to login for ${principal.get}", e)
          None
      }
    }
  }

  private def zkAuthKeytabFileConf(sparkConf: Map[String, String]): Map[String, String] = {
    val zkAuthKeytab = conf.get(HighAvailabilityConf.HA_ZK_AUTH_KEYTAB)
    if (zkAuthKeytab.isDefined) {
      sparkConf.get(SPARK_FILES) match {
        case Some(files) =>
          Map(SPARK_FILES -> s"$files,${zkAuthKeytab.get}")
        case _ =>
          Map(SPARK_FILES -> zkAuthKeytab.get)
      }
    } else {
      Map()
    }
  }

  override def shortName: String = "spark"

  override def getEngineHome(shortName: String): String = cluster match {
    case Some(clu) =>
      val homeKey = s"${shortName.toUpperCase}_HOME"
      env.get(homeKey) match {
        case Some(homeVal) => homeVal + "/" + clu.getCommand.getCommand
        case None => throw validateEnv(homeKey)
      }
    case _ => super.getEngineHome(shortName)
  }

  protected lazy val defaultMaster: Option[String] = {
    val confDir = env.getOrElse(SPARK_CONF_DIR, s"$sparkHome${File.separator}conf")
    val defaults =
      try {
        val confFile = new File(s"$confDir${File.separator}$SPARK_CONF_FILE_NAME")
        if (confFile.exists()) {
          Utils.getPropertiesFromFile(Some(confFile))
        } else {
          Map.empty[String, String]
        }
      } catch {
        case _: Exception =>
          warn(s"Failed to load spark configurations from $confDir")
          Map.empty[String, String]
      }
    defaults.get(MASTER_KEY)
  }

  override def clusterManager(): Option[String] = cluster match {
    case Some(clu) => Some(clu.getMaster)
    case _ => conf.getOption(MASTER_KEY).orElse(defaultMaster)
  }

  override def validateConf: Unit = Validator.validateConf(conf)

  // For spark on kubernetes, spark pod using env SPARK_USER_NAME as current user
  def setSparkUserName(userName: String, buffer: ArrayBuffer[String]): Unit = {
    clusterManager().foreach { cm =>
      if (cm.toUpperCase.startsWith("K8S")) {
        buffer += CONF
        buffer += s"spark.kubernetes.driverEnv.SPARK_USER_NAME=$userName"
        buffer += CONF
        buffer += s"spark.executorEnv.SPARK_USER_NAME=$userName"
      }
    }
  }
}

object SparkProcessBuilder {
  final val APP_KEY = "spark.app.name"
  final val TAG_KEY = "spark.yarn.tags"
  final val MASTER_KEY = "spark.master"
  final val INTERNAL_RESOURCE = "spark-internal"
  final val SPARK_CONFIG_FILE_KEY = "--properties-file"
  final val SPARK_SUBMIT_OPTS = "SPARK_SUBMIT_OPTS"
  final val SPARK_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label."
  final val SPARK_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label."
  final val SPARK_HADOOP_CONF_PREFIX = "spark.hadoop."
  final val JOB_ID_LABEL_KEY = "genie-job-id"
  final val JOB_NAME_LABEL_KEY = "genie-job-name"
  final val SPARK_HOME = "SPARK_HOME"
  final val SPARK_WAIT_COMPLETION_KEY = "spark.kubernetes.submission.waitAppCompletion"
  final val HW_OBS_ACCESS_ID_KEY = "spark.hadoop.fs.obs.access.key"
  final val HW_OBS_SECRET_KEY_KEY = "spark.hadoop.fs.obs.secret.key"
  final val AWS_S3A_ACCESS_ID_KEY = "spark.hadoop.fs.s3a.access.key"
  final val AWS_S3A_SECRET_KEY_KEY = "spark.hadoop.fs.s3a.secret.key"
  final val AWS_S3_ACCESS_ID_KEY = "spark.hadoop.fs.s3.access.key"
  final val AWS_S3_SECRET_KEY_KEY = "spark.hadoop.fs.s3.secret.key"

  /**
   * The path configs from Spark project that might upload local files:
   * - SparkSubmit
   * - org.apache.spark.deploy.yarn.Client::prepareLocalResources
   * - KerberosConfDriverFeatureStep::configurePod
   * - KubernetesUtils.uploadAndTransformFileUris
   */
  final val PATH_CONFIGS = Seq(
    SPARK_FILES,
    "spark.jars",
    "spark.archives",
    "spark.yarn.jars",
    "spark.yarn.dist.files",
    "spark.yarn.dist.pyFiles",
    "spark.submit.pyFiles",
    "spark.yarn.dist.jars",
    "spark.yarn.dist.archives",
    "spark.kerberos.keytab",
    "spark.yarn.keytab",
    "spark.kubernetes.kerberos.krb5.path",
    SPARK_KUBERNETES_UPLOAD_KEY)

  final private[spark] val CONF = "--conf"
  final private[spark] val CLASS = "--class"
  final private[spark] val PROXY_USER = "--proxy-user"
  final private[spark] val SPARK_FILES = "spark.files"
  final private[spark] val PRINCIPAL = "spark.kerberos.principal"
  final private[spark] val KEYTAB = "spark.kerberos.keytab"
  final private[spark] val SPARK_KUBERNETES_UPLOAD_KEY = "spark.kubernetes.file.upload.path"

  // Get the appropriate spark-submit file
  final private val SPARK_SUBMIT_FILE = if (Utils.isWindows) "spark-submit.cmd" else "spark-submit"
  final private val SPARK_CONF_DIR = "SPARK_CONF_DIR"
  final private val SPARK_CONF_FILE_NAME = "spark-defaults.conf"
  final private val GATEWAY_SPARK_MAIN_RESOURCE_KEY = "gateway.spark.mainResource"
}
