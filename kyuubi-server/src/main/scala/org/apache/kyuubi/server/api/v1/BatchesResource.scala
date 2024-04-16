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

package org.apache.kyuubi.server.api.v1

import java.io.InputStream
import java.nio.file.{Files, Paths}
import java.sql.SQLException
import java.time.Duration
import java.util
import java.util.{Collections, Locale, UUID}
import java.util.concurrent.ConcurrentHashMap
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response.Status

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.commons.lang3.StringUtils
import org.glassfish.jersey.media.multipart.{FormDataContentDisposition, FormDataParam}

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.client.util.BatchUtils._
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{KYUUBI_AUTHORIZATION_ENABLED, KYUUBI_CLUSTER_CONFIG_PATH}
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.engine.{ApplicationInfo, KyuubiApplicationManager, ProcBuilder, ResourceType}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.operation.{BatchJobSubmission, FetchOrientation, OperationState}
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.api.v1.BatchesResource._
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.storage.{ObjectStorageType, ObjectStorageUrlParser}
import org.apache.kyuubi.util.{JdbcUtils, ObjectStorageUtil, SqlUtils, TagsParser}

@Tag(name = "Batch")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class BatchesResource extends ApiRequestContext with Logging {
  private val internalRestClients = new ConcurrentHashMap[String, InternalRestClient]()
  private lazy val internalSocketTimeout =
    fe.getConf.get(KyuubiConf.BATCH_INTERNAL_REST_CLIENT_SOCKET_TIMEOUT)
  private lazy val internalConnectTimeout =
    fe.getConf.get(KyuubiConf.BATCH_INTERNAL_REST_CLIENT_CONNECT_TIMEOUT)

  private def getInternalRestClient(kyuubiInstance: String): InternalRestClient = {
    internalRestClients.computeIfAbsent(
      kyuubiInstance,
      kyuubiInstance => {
        new InternalRestClient(
          kyuubiInstance,
          internalSocketTimeout.toInt,
          internalConnectTimeout.toInt)
      })
  }

  private def sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private def buildBatch(session: KyuubiBatchSessionImpl): Batch = {
    val batchOp = session.batchJobSubmissionOp
    val batchOpStatus = batchOp.getStatus
    val batchAppStatus = batchOp.getOrFetchCurrentApplicationInfo

    val name = Option(batchOp.batchName).getOrElse(batchAppStatus.map(_.name).orNull)
    var appId: String = null
    var appUrl: String = null
    var appState: String = null
    var appDiagnostic: String = null
    var clusterId: String = null

    if (batchAppStatus.nonEmpty) {
      appId = batchAppStatus.get.id
      appUrl = batchAppStatus.get.url.orNull
      appState = batchAppStatus.get.state.toString
      appDiagnostic = batchAppStatus.get.error.orNull
      clusterId = batchAppStatus.get.clusterId.orNull
    } else {
      val metadata = sessionManager.getBatchMetadata(batchOp.batchId)
      if (null != metadata) {
        appId = metadata.engineId
        appUrl = metadata.engineUrl
        appState = metadata.engineState
        appDiagnostic = metadata.engineError.orNull
        clusterId = metadata.clusterManager.orNull
      }
    }

    new Batch(
      batchOp.batchId,
      session.user,
      batchOp.batchType,
      name,
      batchOp.appStartTime,
      appId,
      appUrl,
      appState,
      appDiagnostic,
      fe.connectionUrl,
      clusterId,
      batchOpStatus.state.toString,
      session.createTime,
      batchOpStatus.completed,
      Map.empty[String, String].asJava)
  }

  private def buildBatch(
      metadata: Metadata,
      batchAppStatus: Option[ApplicationInfo]): Batch = {
    batchAppStatus.map { appStatus =>
      val currentBatchState =
        if (BatchJobSubmission.applicationFailed(batchAppStatus)) {
          OperationState.ERROR.toString
        } else if (BatchJobSubmission.applicationTerminated(batchAppStatus)) {
          OperationState.FINISHED.toString
        } else if (batchAppStatus.isDefined) {
          OperationState.RUNNING.toString
        } else {
          metadata.state
        }

      val name = Option(metadata.requestName).getOrElse(appStatus.name)
      val appId = appStatus.id
      val appUrl = appStatus.url.orNull
      val appState = appStatus.state.toString
      val appDiagnostic = appStatus.error.orNull

      new Batch(
        metadata.identifier,
        metadata.username,
        metadata.engineType,
        name,
        metadata.engineOpenTime,
        appId,
        appUrl,
        appState,
        appDiagnostic,
        metadata.kyuubiInstance,
        metadata.clusterManager.orNull,
        currentBatchState,
        metadata.createTime,
        metadata.endTime,
        Map.empty[String, String].asJava)
    }.getOrElse(MetadataManager.buildBatch(metadata))
  }

  private def formatSessionHandle(sessionHandleStr: String): SessionHandle = {
    try {
      SessionHandle.fromUUID(sessionHandleStr)
    } catch {
      case e: IllegalArgumentException =>
        throw new NotFoundException(s"Invalid batchId: $sessionHandleStr", e)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Batch]))),
    description = "create and open a batch session")
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def openBatchSession(request: BatchRequest): Batch = {
    openBatchSessionInternal(request)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Batch]))),
    description = "create and open a batch session with uploading resource file")
  @POST
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def openBatchSessionWithUpload(
      @FormDataParam("batchRequest") batchRequest: BatchRequest,
      @FormDataParam("resourceFile") resourceFileInputStream: InputStream,
      @FormDataParam("resourceFile") resourceFileMetadata: FormDataContentDisposition): Batch = {
    require(
      batchRequest != null,
      "batchRequest is required and please check the content type" +
        " of batchRequest is application/json")
    val tempFile = Utils.writeToTempFile(
      resourceFileInputStream,
      KyuubiApplicationManager.uploadWorkDir,
      resourceFileMetadata.getFileName)
    batchRequest.setResource(tempFile.getPath)
    openBatchSessionInternal(batchRequest, isResourceFromUpload = true)
  }

  /**
   * open new batch session with request
   *
   * @param request              instance of BatchRequest
   * @param isResourceFromUpload whether to clean up temporary uploaded resource file
   *                             in local path after execution
   */
  private def openBatchSessionInternal(
      request: BatchRequest,
      isResourceFromUpload: Boolean = false): Batch = {
    require(
      supportedBatchType(request.getBatchType),
      s"${request.getBatchType} is not in the supported list: $SUPPORTED_BATCH_TYPES")
    require(
      request.getResource != null || unnecessaryResource(request.getBatchType),
      "resource is a required parameter")
    if (request.getBatchType.equalsIgnoreCase("SPARK")) {
      require(request.getClassName != null, "classname is a required parameter for SPARK")
    }
    request.setBatchType(request.getBatchType.toUpperCase(Locale.ROOT))
    request.getConf.put(KYUUBI_SESSION_REAL_USER_KEY, fe.getRealUser())

    val sql = SqlUtils.parseSqlFromArgs(request.getArgs.toSeq)
    val engineName = SqlUtils.parseEngineType(request.getBatchType)
    val needAuth = StringUtils.isNotBlank(sql) && SqlUtils.needAuth(
      sessionManager.getConf.get(KYUUBI_AUTHORIZATION_ENABLED),
      request.getBatchType) && false
    // disable auth at here
    if (needAuth) {
      try {
        val sqlInfo = SqlUtils.authSql(
          sessionManager.authorizationManager,
          sql,
          engineName,
          request.getConf.asScala)
        // skip sqlInfo is null
        if (null != sqlInfo && !sqlInfo.getPermit) {
          val error = SqlUtils.packAuthError(
            request.getConf.getOrDefault(KYUUBI_SESSION_REAL_USER_KEY, ""),
            sql,
            sqlInfo)
          throw new SQLException(error)
        }
      } catch {
        case e: Exception =>
          throw new NotAllowedException("SQL authorization failed", e)
      }
    }

    val userProvidedBatchId = request.getConf.asScala.get(KYUUBI_BATCH_ID_KEY)
    userProvidedBatchId.foreach { batchId =>
      try UUID.fromString(batchId)
      catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"$KYUUBI_BATCH_ID_KEY=$batchId must be an UUID", e)
      }
    }

    userProvidedBatchId.flatMap { batchId =>
      // use for ninebot by smy
      downloadResource(request, batchId)
      Option(sessionManager.getBatchFromMetadataStore(batchId))
    } match {
      case Some(batch) =>
        markDuplicated(batch)
      case None =>
        val userName = fe.getSessionUser(request.getConf.asScala.toMap)
        val ipAddress = fe.getIpAddress
        val batchId = userProvidedBatchId.getOrElse(UUID.randomUUID().toString)

        // use for ninebot by smy
        downloadResource(request, batchId)
        request.setConf(
          (request.getConf.asScala ++ Map(
            KYUUBI_BATCH_ID_KEY -> batchId,
            KYUUBI_BATCH_RESOURCE_UPLOADED_KEY -> isResourceFromUpload.toString,
            KYUUBI_CLIENT_IP_KEY -> ipAddress,
            KYUUBI_SERVER_IP_KEY -> fe.host,
            KYUUBI_SESSION_CONNECTION_URL_KEY -> fe.connectionUrl,
            KYUUBI_SESSION_REAL_USER_KEY -> fe.getRealUser(),
            SparkProcessBuilder.SPARK_WAIT_COMPLETION_KEY -> "false")).asJava)

        Try {
          sessionManager.openBatchSession(
            userName,
            "anonymous",
            ipAddress,
            request.getConf.asScala.toMap,
            request)
        } match {
          case Success(sessionHandle) =>
            buildBatch(sessionManager.getBatchSessionImpl(sessionHandle))
          case Failure(cause) if JdbcUtils.isDuplicatedKeyDBErr(cause) =>
            val batch = sessionManager.getBatchFromMetadataStore(batchId)
            assert(batch != null, s"can not find duplicated batch $batchId from metadata store")
            markDuplicated(batch)
        }
    }
  }

  private def markDuplicated(batch: Batch): Batch = {
    warn(s"duplicated submission: ${batch.getId}, ignore and return the existing batch.")
    batch.setBatchInfo(Map(KYUUBI_BATCH_DUPLICATED_KEY -> "true").asJava)
    batch
  }

  private def download(res: String, region: String, batchId: String): String = {
    val parser = ObjectStorageUrlParser.parser(res, true)
    try {
      parser.getStorageType match {
        case ObjectStorageType.HDFS | ObjectStorageType.FILE | ObjectStorageType.LOCAL => return res
        case _ =>
      }
    } catch {
      case _: Throwable => return res
    }
    val destFile = Paths.get(ProcBuilder.createSessionTmpPath(batchId), parser.getFilename)
    if (Files.notExists(destFile.getParent)) {
      Files.createDirectories(destFile.getParent)
    }

    ObjectStorageUtil.withCloseable(ObjectStorageUtil.build(res, region, fe.getConf)) {
      storage =>
        return storage.getObject(
          parser.getBucketName,
          parser.getObjectKey,
          destFile.toString).getAbsolutePath
    }
  }

  private def downloadResource(req: BatchRequest, batchId: String): Unit = {
    if (!needDownloadResource(req.getBatchType)) {
      return
    }
    val clusterTags = req.getConf.getOrDefault(KyuubiConf.KYUUBI_SESSION_CLUSTER_TAGS.key, "")
    val region = TagsParser.getValueFromTags(clusterTags, "region")
    val mainRes = download(req.getResource.trim, region, batchId)
    req.setResource(mainRes)

    val sb = new StringBuilder

    // spark
    val batchType = req.getBatchType
    if (Seq("SPARK", "PYSPARK").contains(batchType)) {
      if (req.getConf.containsKey("spark.yarn.jars")) {
        val jars = req.getConf.get("spark.yarn.jars").split(",")
        jars.foreach { jar =>
          val res = download(jar.trim, region, batchId)
          sb.append(res).append(",")
        }
        if (sb.nonEmpty) {
          req.getConf.put("spark.yarn.jars", sb.substring(0, sb.size - 1))
        }
      }

      sb.clear
      if (req.getConf.containsKey("spark.yarn.archive")) {
        val archives = req.getConf.get("spark.yarn.archive").split(",")
        archives.foreach { archive =>
          val res = download(archive.trim, region, batchId)
          sb.append(res).append(",")
        }
        if (sb.nonEmpty) {
          req.getConf.put("spark.yarn.archive", sb.substring(0, sb.size - 1))
        }
      }
    }

    // flink
    sb.clear
    if ("FLINK" == batchType) {
      val idx = req.getArgs.indexOf("-yt")
      if (idx >= 0 && idx < req.getArgs.size() - 1) {
        val jars = req.getArgs.get(idx + 1).split(",")
        jars.foreach { jar =>
          val res = download(jar.trim, region, batchId)
          sb.append(res).append(",")
        }
        req.getArgs.set(idx + 1, sb.substring(0, sb.size - 1))
      }
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Batch]))),
    description = "get the batch info via batch id")
  @GET
  @Path("{batchId}")
  def batchInfo(@PathParam("batchId") batchId: String): Batch = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val sessionHandle = formatSessionHandle(batchId)
    val batch = Option(sessionManager.getBatchSessionImpl(sessionHandle)).map { batchSession =>
      buildBatch(batchSession)
    }.getOrElse {
      Option(sessionManager.getBatchMetadata(batchId)).map { metadata =>
        if (OperationState.isTerminal(OperationState.withName(metadata.state)) ||
          metadata.kyuubiInstance == fe.connectionUrl) {
          val batch = MetadataManager.buildBatch(metadata)
          if (StringUtils.isEmpty(batch.getAppUrl) && "FLINK" == metadata.engineType) {

            val batchAppStatus = sessionManager.applicationManager.getApplicationInfo(
              Option(
                sessionManager.getClusterMaster(
                  metadata.engineType,
                  metadata.clusterManager.orNull)),
              batchId,
              Option(new Cluster(
                metadata.clusterManager.orNull,
                fe.getConf.get(KYUUBI_CLUSTER_CONFIG_PATH))),
              None,
              Option(ResourceType.DEPLOYMENT))
            if (batchAppStatus.isDefined) {
              return buildBatch(metadata, batchAppStatus)
            }
          }
          batch
        } else {
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          try {
            internalRestClient.getBatch(userName, batchId)
          } catch {
            case e: KyuubiRestException =>
              error(s"Error redirecting get batch[$batchId] to ${metadata.kyuubiInstance}", e)
              var resType = ResourceType.POD
              if ("FLINK" == metadata.engineType) {
                resType = ResourceType.DEPLOYMENT
              }
              val batchAppStatus = sessionManager.applicationManager.getApplicationInfo(
                Option(sessionManager.getClusterMaster(
                  metadata.engineType,
                  metadata.clusterManager.orNull)),
                batchId,
                Option(new Cluster(
                  metadata.clusterManager.orNull,
                  fe.getConf.get(KYUUBI_CLUSTER_CONFIG_PATH))),
                None,
                Option(resType))
              if (batchAppStatus.isDefined) {
                buildBatch(metadata, batchAppStatus)
              } else {
                MetadataManager.buildBatch(metadata)
              }
          }
        }
      }.getOrElse {
        error(s"Invalid batchId: $batchId")
        throw new NotFoundException(s"Invalid batchId: $batchId")
      }
    }

    batch
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetBatchesResponse]))),
    description = "returns the batch sessions.")
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def getBatchInfoList(
      @QueryParam("name") name: String,
      @QueryParam("batchType") batchType: String,
      @QueryParam("batchState") batchState: String,
      @QueryParam("batchUser") batchUser: String,
      @QueryParam("createTime") createTime: Long,
      @QueryParam("endTime") endTime: Long,
      @QueryParam("from") from: Int,
      @QueryParam("size") @DefaultValue("100") size: Int): GetBatchesResponse = {
    require(
      createTime >= 0 && endTime >= 0 && (endTime == 0 || createTime <= endTime),
      "Invalid time range")
    if (batchState != null) {
      require(
        validBatchState(batchState),
        s"The valid batch state can be one of the following: ${VALID_BATCH_STATES.mkString(",")}")
    }
    val batches =
      sessionManager.getBatchesFromMetadataStore(
        name,
        batchType,
        batchUser,
        batchState,
        createTime,
        endTime,
        from,
        size)
    new GetBatchesResponse(from, batches.size, batches.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationLog]))),
    description = "get the local log lines from this batch")
  @GET
  @Path("{batchId}/localLog")
  def getBatchLocalLog(
      @PathParam("batchId") batchId: String,
      @QueryParam("from") @DefaultValue("-1") from: Int,
      @QueryParam("size") @DefaultValue("100") size: Int): OperationLog = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val sessionHandle = formatSessionHandle(batchId)
    Option(sessionManager.getBatchSessionImpl(sessionHandle)).map { batchSession =>
      try {
        val submissionOp = batchSession.batchJobSubmissionOp
        val rowSet = submissionOp.getOperationLogRowSet(FetchOrientation.FETCH_NEXT, from, size)
        val columns = rowSet.getColumns
        val logRowSet: util.List[String] =
          if (columns == null || columns.size == 0) {
            Collections.emptyList()
          } else {
            assert(columns.size == 1)
            columns.get(0).getStringVal.getValues
          }
        new OperationLog(logRowSet, logRowSet.size)
      } catch {
        case NonFatal(e) =>
          val errorMsg = s"Error getting operation log for batchId: $batchId"
          error(errorMsg, e)
          throw new NotFoundException(errorMsg)
      }
    }.getOrElse {
      Option(sessionManager.getBatchMetadata(batchId)).map { metadata =>
        if (fe.connectionUrl != metadata.kyuubiInstance) {
          info(s"Redirecting localLog batch[$batchId] to ${metadata.kyuubiInstance}")
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          internalRestClient.getBatchLocalLog(userName, batchId, from, size)
        } else {
          val opLog = org.apache.kyuubi.operation.log.OperationLog.findOperationLog(
            sessionManager.operationLogRoot.get,
            metadata.identifier)
          ProcBuilder.findEngineLog(opLog, batchId)
          val rowSet = opLog.read(from, size)
          val columns = rowSet.getColumns
          val logRowSet: util.List[String] =
            if (columns == null || columns.size == 0) {
              Collections.emptyList()
            } else {
              assert(columns.size == 1)
              columns.get(0).getStringVal.getValues
            }
          new OperationLog(logRowSet, logRowSet.size)
        }
      }.getOrElse {
        error(s"No local log found for batch: $batchId")
        throw new NotFoundException(s"No local log found for batch: $batchId")
      }
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[CloseBatchResponse]))),
    description = "close and cancel a batch session")
  @DELETE
  @Path("{batchId}")
  def closeBatchSession(
      @PathParam("batchId") batchId: String,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String): CloseBatchResponse = {
    val sessionHandle = formatSessionHandle(batchId)

    val userName = fe.getSessionUser(hs2ProxyUser)

    Option(sessionManager.getBatchSessionImpl(sessionHandle)).map { batchSession =>
      if (userName != batchSession.user) {
        throw new WebApplicationException(
          s"$userName is not allowed to close the session belong to ${batchSession.user}",
          Status.METHOD_NOT_ALLOWED)
      }
      sessionManager.closeSession(batchSession.handle)
      val (success, msg) = batchSession.batchJobSubmissionOp.getKillMessage
      new CloseBatchResponse(success, msg)
    }.getOrElse {
      Option(sessionManager.getBatchMetadata(batchId)).map { metadata =>
        if (userName != metadata.username) {
          throw new WebApplicationException(
            s"$userName is not allowed to close the session belong to ${metadata.username}",
            Status.METHOD_NOT_ALLOWED)
        } else if (OperationState.isTerminal(OperationState.withName(metadata.state)) ||
          metadata.kyuubiInstance == fe.connectionUrl) {
          if ("FLINK" == metadata.engineType) {
            val appMgrKillResp = sessionManager.applicationManager.killApplication(
              Option(sessionManager.getClusterMaster(
                metadata.engineType,
                metadata.clusterManager.orNull)),
              batchId,
              Option(new Cluster(
                metadata.clusterManager.orNull,
                fe.getConf.get(KYUUBI_CLUSTER_CONFIG_PATH))),
              None,
              Some(ResourceType.DEPLOYMENT))
            return new CloseBatchResponse(appMgrKillResp._1, appMgrKillResp._2)
          }
          new CloseBatchResponse(false, s"The batch[$metadata] has been terminated.")
        } else {
          info(s"Redirecting delete batch[$batchId] to ${metadata.kyuubiInstance}")
          val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
          try {
            internalRestClient.deleteBatch(userName, batchId)
          } catch {
            case e: KyuubiRestException =>
              error(s"Error redirecting delete batch[$batchId] to ${metadata.kyuubiInstance}", e)
              var resType = ResourceType.POD
              if ("FLINK" == metadata.engineType) {
                resType = ResourceType.DEPLOYMENT
              }
              val appMgrKillResp = sessionManager.applicationManager.killApplication(
                Option(sessionManager.getClusterMaster(
                  metadata.engineType,
                  metadata.clusterManager.orNull)),
                batchId,
                Option(new Cluster(
                  metadata.clusterManager.orNull,
                  fe.getConf.get(KYUUBI_CLUSTER_CONFIG_PATH))),
                None,
                Some(resType))
              info(
                s"Marking batch[$batchId/${metadata.kyuubiInstance}] closed by ${fe.connectionUrl}")
              sessionManager.updateMetadata(Metadata(
                identifier = batchId,
                peerInstanceClosed = true))
              if (appMgrKillResp._1) {
                new CloseBatchResponse(appMgrKillResp._1, appMgrKillResp._2)
              } else {
                new CloseBatchResponse(false, Utils.stringifyException(e))
              }
          }
        }
      }.getOrElse {
        error(s"Invalid batchId: $batchId")
        throw new NotFoundException(s"Invalid batchId: $batchId")
      }
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[CloseBatchResponse]))),
    description = "close and cancel a interactive session")
  @DELETE
  @Path("interactive/{sessionId}")
  def closeSessionForInteractive(
      @PathParam("sessionId") sessionId: String,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String): CloseBatchResponse = {
    info(s"CloseSession for interactive: [$sessionId]")
    try {
      val sessionHandle =
        try {
          SessionHandle.fromUUID(sessionId)
        } catch {
          case e: IllegalArgumentException =>
            throw new NotFoundException(s"Invalid sessionId: $sessionId", e)
        }

      val userName = fe.getSessionUser(hs2ProxyUser)

      Option(sessionManager.getInteractiveSessionImpl(sessionHandle)).map { session =>
        if (userName != session.user) {
          throw new WebApplicationException(
            s"$userName is not allowed to close the session belong to ${session.user}",
            Status.METHOD_NOT_ALLOWED)
        }
        sessionManager.closeSession(session.handle)
        new CloseBatchResponse(true, "SUCCESS")
      }.getOrElse {
        Option(sessionManager.getInteractiveMetadata(sessionId)).map { metadata =>
          if (userName != metadata.username) {
            throw new WebApplicationException(
              s"$userName is not allowed to close the session belong to ${metadata.username}",
              Status.METHOD_NOT_ALLOWED)
          } else {
            info(s"Redirecting delete session[$sessionId] to ${metadata.kyuubiInstance}")
            val internalRestClient = getInternalRestClient(metadata.kyuubiInstance)
            try {
              internalRestClient.deleteInteractive(userName, sessionId)
              new CloseBatchResponse(true, "SUCCESS")
            } catch {
              case e: KyuubiRestException =>
                val appMgrKillResp = sessionManager.applicationManager.killApplication(
                  Option(sessionManager.getClusterMaster(
                    metadata.engineType,
                    metadata.clusterManager.orNull)),
                  sessionId,
                  Option(new Cluster(
                    metadata.clusterManager.orNull,
                    fe.getConf.get(KYUUBI_CLUSTER_CONFIG_PATH))))
                info(
                  s"Marking session[$sessionId/${metadata.kyuubiInstance}] " +
                    s"closed by ${fe.connectionUrl}")
                sessionManager.updateMetadata(Metadata(
                  identifier = sessionId,
                  peerInstanceClosed = true))
                if (appMgrKillResp._1) {
                  new CloseBatchResponse(appMgrKillResp._1, appMgrKillResp._2)
                } else {
                  new CloseBatchResponse(false, Utils.stringifyException(e))
                }
            }
          }
        }.getOrElse {
          error(s"Invalid sessionId: $sessionId")
          throw new NotFoundException(s"Invalid sessionId: $sessionId")
        }
      }
    } catch {
      case ex: Exception =>
        error(s"kill session: $sessionId failed ${ex.getMessage}")
        new CloseBatchResponse(false, ex.getMessage)
    }
  }

  private def uploadFileReady(sessionId: String): Boolean = {
    val session = sessionManager.getInteractiveMetadata(sessionId)
    null != session && OperationState.UPLOADED == OperationState.withName(session.state)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[SignatureUrlResponse]))),
    description = "get the batch info via batch id")
  @GET
  @Path("{batchId}/signature")
  def signature(
      @PathParam("batchId") batchId: String,
      @QueryParam("output") output: String,
      @QueryParam("region") region: String,
      @QueryParam("suffix") @DefaultValue(".csv") suffix: String,
      @QueryParam("expireDays") @DefaultValue("7") expireDays: Long,
      @QueryParam("force") @DefaultValue("false") force: Boolean): SignatureUrlResponse = {
    require(StringUtils.isNotBlank(output), "Invalid Parameter: output is empty")
    if (!force && !uploadFileReady(batchId)) {
      return SignatureUrlResponse.EMPTY
    }
    try {
      val parser = ObjectStorageUrlParser.parser(prefix(output, batchId))
      ObjectStorageUtil.withCloseable(ObjectStorageUtil.build(
        output,
        region,
        fe.getConf)) {
        storage =>
          val objs = storage.listObjects(parser.getBucketName, parser.getPrefix, suffix)
          val resp = new SignatureUrlResponse(true, "SUCCESS")
          objs.foreach { obj =>
            val ss = obj.split("/")
            if (ss.length >= 3) {
              val len = ss.length
              val operationId = ss(len - 2)
              val url = storage.signObject(parser.getBucketName, obj, Duration.ofDays(expireDays))
              resp.putUrl(operationId, url)
            }
          }
          resp
      }
    } catch {
      case e: Exception =>
        new SignatureUrlResponse(false, e.getMessage, null)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetDataSizeResponse]))),
    description = "get the batch info via batch id")
  @GET
  @Path("{batchId}/dataSize")
  def dataSize(
      @PathParam("batchId") batchId: String,
      @QueryParam("output") output: String,
      @QueryParam("region") region: String,
      @QueryParam("suffix") @DefaultValue(".csv") suffix: String,
      @QueryParam("force") @DefaultValue("false") force: Boolean): GetDataSizeResponse = {
    require(StringUtils.isNotBlank(output), "Invalid Parameter: output is empty")
    if (!force && !uploadFileReady(batchId)) {
      return GetDataSizeResponse.EMPTY
    }
    val parser = ObjectStorageUrlParser.parser(prefix(output, batchId))
    ObjectStorageUtil.withCloseable(
      ObjectStorageUtil.build(output, region, fe.getConf)) {
      storage =>
        val size = storage.objectsSize(
          parser.getBucketName,
          parser.getPrefix,
          suffix)
        new GetDataSizeResponse(size)
    }
  }

  def prefix(output: String, batchId: String): String = if (output.endsWith("/")) {
    output + batchId
  } else {
    output + "/" + batchId
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ExplainResponse]))),
    description = "explain sql")
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("explain")
  def liteExplain(request: ExplainRequest): ExplainResponse = {
    val userName = fe.getRealUser()
    var sql = request.getSql
    if (StringUtils.isBlank(sql)) {
      return new ExplainResponse(false, "sql is empty")
    }
    if (SqlUtils.isBase64(sql)) {
      sql = SqlUtils.decodeSql(sql)
    }
    sessionManager.authorizationManager match {
      case Some(am) =>
        try {
          val sqlInfo = am.auth(
            sql,
            userName,
            request.getTenant,
            request.getCatalog,
            request.getDatabase,
            SqlUtils.parseEngineType(request.getEngineType))
          // skip sqlInfo is null
          if (null != sqlInfo && !sqlInfo.getPermit) {
            val error = SqlUtils.packAuthError(userName, sql, sqlInfo)
            return new ExplainResponse(false, error)
          }
        } catch {
          case e: Exception =>
            return new ExplainResponse(false, e.getMessage)
        }
        new ExplainResponse(true, "SUCCESS")
      case None =>
        new ExplainResponse(false, "AuthorizationManager has not enabled")
    }
  }

}

object BatchesResource {
  val SUPPORTED_BATCH_TYPES: Seq[String] =
    Seq("SPARK", "PYSPARK", "SCRIPT", "TFJOB", "HIVE", "FLINK")
  val VALID_BATCH_STATES: Seq[String] = Seq(
    OperationState.PENDING,
    OperationState.RUNNING,
    OperationState.FINISHED,
    OperationState.ERROR,
    OperationState.CANCELED).map(_.toString)

  def supportedBatchType(batchType: String): Boolean = {
    Option(batchType).exists(bt => SUPPORTED_BATCH_TYPES.contains(bt.toUpperCase(Locale.ROOT)))
  }

  def unnecessaryResource(batchType: String): Boolean = {
    Option(batchType).exists(bt => Array("HIVE", "FLINK").contains(bt.toUpperCase(Locale.ROOT)))
  }

  def needDownloadResource(batchType: String): Boolean = {
    Option(batchType).exists(bt =>
      Array("SPARK", "PYSPARK", "SCRIPT", "TFJOB", "FLINK").contains(bt.toUpperCase(Locale.ROOT)))
    false
  }

  def validBatchState(batchState: String): Boolean = {
    Option(batchState).exists(bt => VALID_BATCH_STATES.contains(bt.toUpperCase(Locale.ROOT)))
  }

}
