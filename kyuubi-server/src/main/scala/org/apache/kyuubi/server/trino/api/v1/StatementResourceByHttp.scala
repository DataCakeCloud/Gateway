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

package org.apache.kyuubi.server.trino.api.v1

import javax.ws.rs._
import javax.ws.rs.core._
import javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE
import javax.ws.rs.core.Response.Status.BAD_REQUEST

import scala.util.Try

import io.airlift.units.Duration
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import io.trino.client.QueryResults
import okhttp3.OkHttpClient
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.server.trino.api._
import org.apache.kyuubi.server.trino.api.v1.dto.Ok
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.util.HttpUtil

@Tag(name = "Statement")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StatementResourceByHttp extends ApiRequestContext with Logging {

  lazy val sm: KyuubiSessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "test")
  @GET
  @Path("test")
  def test(): Ok = new Ok("trino server is running")

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[QueryResults]))),
    description =
      "Create a query")
  @POST
  @Path("/")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def query(
      statement: String,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {
    if (statement == null || statement.isEmpty) {
      throw badRequest(BAD_REQUEST, "SQL statement is empty")
    }

    info(s"trino statement: $statement, headers: [${headers.getRequestHeaders}], " +
      s"uriInfo: [${uriInfo.getRequestUri}]")

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)
    try {
      val cluster = selectCluster(uriInfo)
      val reqHeader = TrinoContext.buildTrinoHeader(headers)
      info(s"trino statement reqHeader: $reqHeader")

      val (respHeader, respBody) =
        HttpUtil.post(cluster.getMaster + uriInfo.getRequestUri.getPath, reqHeader, statement)

      TrinoContext.buildTrinoResponse(respHeader, respBody, uriInfo, trinoContext)
    } catch {
      case e: Exception =>
        throw badRequest(BAD_REQUEST, e.getMessage)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get queued statement status")
  @GET
  @Path("/queued/{queryId}/{slug}/{token}")
  def getQueuedStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @QueryParam("maxWait") maxWait: Duration,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {

    debug(s"trino getQueuedStatementStatus queryId: [$queryId], slug: [$slug], " +
      s"token: [$token], maxWait: [$maxWait], headers: [${headers.getRequestHeaders}], " +
      s"uriInfo: [${uriInfo.getRequestUri}]")

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)

    try {
      val cluster = selectCluster(uriInfo)
      val h = TrinoContext.buildTrinoHeader(headers)
      val url = TrinoContext.replaceSchemeAndHost(
        uriInfo.getRequestUri,
        cluster.getMasterScheme,
        cluster.getMasterHost)
      val (respHeader, respBody) = HttpUtil.get(url, h)

      TrinoContext.buildTrinoResponse(respHeader, respBody, uriInfo, trinoContext)
    } catch {
      case e: Exception =>
        throw badRequest(BAD_REQUEST, e.getMessage)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get executing statement status")
  @GET
  @Path("/executing/{queryId}/{slug}/{token}")
  def getExecutingStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @QueryParam("maxWait") maxWait: Duration,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {

    debug(s"trino getExecutingStatementStatus queryId: [$queryId], slug: [$slug], " +
      s"token: [$token], maxWait: [$maxWait], headers: [${headers.getRequestHeaders}], " +
      s"uriInfo: [${uriInfo.getRequestUri}]")

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)

    try {
      val cluster = selectCluster(uriInfo)
      val h = TrinoContext.buildTrinoHeader(headers)
      val url = TrinoContext.replaceSchemeAndHost(
        uriInfo.getRequestUri,
        cluster.getMasterScheme,
        cluster.getMasterHost)
      val (respHeader, respBody) = HttpUtil.get(url, h)

      TrinoContext.buildTrinoResponse(respHeader, respBody, uriInfo, trinoContext)
    } catch {
      case e: Exception =>
        throw badRequest(BAD_REQUEST, e.getMessage)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Cancel queued statement")
  @DELETE
  @Path("/queued/{queryId}/{slug}/{token}")
  def cancelQueuedStatement(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {

    debug(s"trino cancelQueuedStatement queryId: [$queryId], slug: [$slug], " +
      s"token: [$token], headers: [${headers.getRequestHeaders}]")

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)

    try {
      val cluster = selectCluster(uriInfo)
      val h = TrinoContext.buildTrinoHeader(headers)
      val url = TrinoContext.replaceSchemeAndHost(
        uriInfo.getRequestUri,
        cluster.getMasterScheme,
        cluster.getMasterHost)
      val (respHeader, respBody) = HttpUtil.delete(url, h)

      TrinoContext.buildTrinoResponse(respHeader, respBody, uriInfo, trinoContext)
    } catch {
      case e: Exception =>
        throw badRequest(BAD_REQUEST, e.getMessage)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Cancel executing statement")
  @DELETE
  @Path("/executing/{queryId}/{slug}/{token}")
  def cancelExecutingStatementStatus(
      @PathParam("queryId") queryId: String,
      @PathParam("slug") slug: String,
      @PathParam("token") token: Long,
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo): Response = {

    debug(s"trino cancelExecutingStatementStatus queryId: [$queryId], slug: [$slug], " +
      s"token: [$token], headers: [${headers.getRequestHeaders}]")

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)

    try {
      val cluster = selectCluster(uriInfo)
      val h = TrinoContext.buildTrinoHeader(headers)
      val url = TrinoContext.replaceSchemeAndHost(
        uriInfo.getRequestUri,
        cluster.getMasterScheme,
        cluster.getMasterHost)
      val (respHeader, respBody) = HttpUtil.delete(url, h)

      TrinoContext.buildTrinoResponse(respHeader, respBody, uriInfo, trinoContext)
    } catch {
      case e: Exception =>
        throw badRequest(BAD_REQUEST, e.getMessage)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Get current status")
  @GET
  @Path("/ui/api/query/state/{queryId}")
  def getCurrentStatus(
      @PathParam("queryId") queryId: String,
      @QueryParam("clusterTags") clusterTags: String): String = {
    require(StringUtils.isNotBlank(clusterTags), "Invalid Parameter: clusterTags is empty")
    val cluster = sm.selectCluster(
      queryId,
      "trino",
      null,
      null,
      Option(clusterTags),
      None,
      None,
      null,
      dynamic = false)

    cluster match {
      case Some(clu) =>
        var master = clu.getMaster
        if (master.endsWith("/")) {
          master = master.substring(0, master.length - 1)
        }
        val url = master + httpRequest.getRequestURI.replace("/v1/statement", "")
        val client = new OkHttpClient.Builder().build()
        val req = new okhttp3.Request.Builder().url(url).get().build()
        var resp: okhttp3.Response = null
        try {
          resp = client.newCall(req).execute()
          if (resp.code - 200 > 100) {
            throw badRequest(
              Response.Status.BAD_REQUEST,
              s"Request trino server response code is [${resp.code}] not 200, " +
                s"msg: [${resp.message}]")
          }
          resp.body().string()
        } finally {
          if (null != resp) {
            resp.close()
          }
        }
      case _ =>
        throw badRequest(Response.Status.BAD_REQUEST, "clusterTags can not match cluster")
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description =
      "Cancel query")
  @DELETE
  @Path("/api/query/{queryId}")
  def cancelQuery(
      @PathParam("queryId") queryId: String,
      @QueryParam("clusterTags") clusterTags: String,
      @Context headers: HttpHeaders): String = {
    require(StringUtils.isNotBlank(clusterTags), "Invalid Parameter: clusterTags is empty")
    val cluster = sm.selectCluster(
      queryId,
      "trino",
      null,
      null,
      Option(clusterTags),
      None,
      None,
      null,
      dynamic = false)

    cluster match {
      case Some(clu) =>
        var master = clu.getMaster
        if (master.endsWith("/")) {
          master = master.substring(0, master.length - 1)
        }
        val url = master + httpRequest.getRequestURI.replace("/statement/api", "")
        info(s"cancel uri $url")
        val authStr = "Authorization"
        val headValue = headers.getHeaderString(authStr)
        val client = new OkHttpClient.Builder().build()
        val req = new okhttp3.Request.Builder().url(url).delete()
          .addHeader(authStr, headValue).build()
        var resp: okhttp3.Response = null
        try {
          resp = client.newCall(req).execute()
          if (resp.code - 200 > 100) {
            throw badRequest(
              Response.Status.BAD_REQUEST,
              s"Request trino server response code is [${resp.code}] not 200, " +
                s"msg: [${resp.message}]")
          }
          resp.body().string()
        } finally {
          if (null != resp) {
            resp.close()
          }
        }
      case _ =>
        throw badRequest(Response.Status.BAD_REQUEST, "clusterTags can not match cluster")
    }
  }

  private def getQuery(
      be: BackendService,
      context: TrinoContext,
      queryId: QueryId,
      slug: String,
      token: Long,
      slugContext: Slug.Context.Context): Try[Query] = {
    Try(be.sessionManager.operationManager.getOperation(queryId.operationHandle)).map { op =>
      val sessionWithId = context.session ++
        Map(Query.KYUUBI_SESSION_ID -> op.getSession.handle.identifier.toString)
      Query(queryId, context.copy(session = sessionWithId), be)
    }.filter(_.getSlug.isValid(slugContext, slug, token))
  }

  private def badRequest(status: Response.Status, message: String) =
    new WebApplicationException(
      Response.status(status)
        .`type`(TEXT_PLAIN_TYPE)
        .entity(message)
        .build)

  private def selectCluster(uriInfo: UriInfo): Cluster = {
    val host = uriInfo.getRequestUri.getHost
    if (null == host) {
      throw badRequest(BAD_REQUEST, s"not support host[$host] to request")
    }
    val clusterTags = s"host:$host"
    val cluster = sm.selectCluster(
      "",
      "trino",
      null,
      null,
      Option(clusterTags),
      None,
      None,
      null,
      dynamic = false)
    if (cluster.isEmpty) {
      throw badRequest(BAD_REQUEST, s"not support host[$host] to request")
    }
    cluster.get
  }

}
