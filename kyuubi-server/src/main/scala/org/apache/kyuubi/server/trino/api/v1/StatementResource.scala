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
import javax.ws.rs.core.Response.Status.{BAD_REQUEST, NOT_FOUND}

import scala.util.Try
import scala.util.control.NonFatal

import io.airlift.units.Duration
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import io.trino.client.QueryResults
import okhttp3.OkHttpClient
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.trino.api._
import org.apache.kyuubi.server.trino.api.Slug.Context.{EXECUTING_QUERY, QUEUED_QUERY}
import org.apache.kyuubi.server.trino.api.v1.dto.Ok
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.KyuubiSessionManager

@Tag(name = "Statement")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StatementResource extends ApiRequestContext with Logging {

  lazy val translator = new KyuubiTrinoOperationTranslator(fe.be)
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
    var host = headers.getRequestHeaders.getFirst("Host")
    if (null == host) {
      host = ""
    } else {
      val idx = host.indexOf(":")
      if (idx > 0) {
        host = host.substring(0, idx)
      }
    }

    try {
      val query = Query(statement, host, trinoContext, translator, fe.be)
      val qr = query.getQueryResults(query.getLastToken, uriInfo)
      TrinoContext.buildTrinoResponse(qr, query.context)
    } catch {
      case e: Exception =>
        val errorMsg =
          s"Error submitting sql"
        error(errorMsg, e)
        throw badRequest(BAD_REQUEST, errorMsg + "\n" + e.getMessage)
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
    val waitTime = if (maxWait == null) 0 else maxWait.toMillis
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, QUEUED_QUERY)
      .flatMap(query =>
        Try(TrinoContext.buildTrinoResponse(
          query.getQueryResults(
            token,
            uriInfo,
            waitTime),
          query.context)))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get
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
    val waitTime = if (maxWait == null) 0 else maxWait.toMillis
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, EXECUTING_QUERY)
      .flatMap(query =>
        Try(TrinoContext.buildTrinoResponse(
          query.getQueryResults(token, uriInfo, waitTime),
          query.context)))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get
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
      @Context headers: HttpHeaders): Response = {

    debug(s"trino cancelQueuedStatement queryId: [$queryId], slug: [$slug], " +
      s"token: [$token], headers: [${headers.getRequestHeaders}]")

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, QUEUED_QUERY)
      .flatMap(query => Try(query.cancel))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get
    Response.noContent.build
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
      @Context headers: HttpHeaders): Response = {

    debug(s"trino cancelExecutingStatementStatus queryId: [$queryId], slug: [$slug], " +
      s"token: [$token], headers: [${headers.getRequestHeaders}]")

    val remoteAddr = Option(httpRequest.getRemoteAddr)
    val trinoContext = TrinoContext(headers, remoteAddr)
    getQuery(fe.be, trinoContext, QueryId(queryId), slug, token, EXECUTING_QUERY)
      .flatMap(query => Try(query.cancel))
      .recover {
        case NonFatal(e) =>
          val errorMsg =
            s"Error executing for query id $queryId"
          error(errorMsg, e)
          throw badRequest(NOT_FOUND, "Query not found")
      }.get

    Response.noContent.build
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

}
