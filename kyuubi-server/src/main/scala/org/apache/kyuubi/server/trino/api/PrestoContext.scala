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

package org.apache.kyuubi.server.trino.api

import javax.ws.rs.core.HttpHeaders

import scala.collection.JavaConverters._

import io.trino.client._
import io.trino.client.ProtocolHeaders.TRINO_HEADERS

import org.apache.kyuubi.server.trino.api.TrinoContext.urlDecode

// TODO: Support replace `preparedStatement` for Trino-jdbc
/**
 * The description and functionality of trino request
 * and response's context
 *
 * @param user               Specifies the session user, must be supplied with every query
 * @param timeZone           The timezone for query processing
 * @param clientCapabilities Exclusive for trino server
 * @param source             This supplies the name of the software that submitted the query,
 *                           e.g. `trino-jdbc` or `trino-cli` by default
 * @param catalog            The catalog context for query processing, will be set response
 * @param schema             The schema context for query processing
 * @param language           The language to use when processing the query and formatting results,
 *                           formatted as a Java Locale string, e.g., en-US for US English
 * @param traceToken         Trace token for correlating requests across systems
 * @param clientInfo         Extra information about the client
 * @param clientTags         Client tags for selecting resource groups. Example: abc,xyz
 * @param preparedStatement  `preparedStatement` are kv pairs, where the names
 *                           are names of previously prepared SQL statements,
 *                           and the values are keys that identify the
 *                           executable form of the named prepared statements
 */
class PrestoContext(
    override val user: String,
    override val timeZone: Option[String] = None,
    override val clientCapabilities: Option[String] = None,
    override val source: Option[String] = None,
    override val catalog: Option[String] = None,
    override val schema: Option[String] = None,
    override val remoteUserAddress: Option[String] = None,
    override val language: Option[String] = None,
    override val traceToken: Option[String] = None,
    override val clientInfo: Option[String] = None,
    override val clientTags: Set[String] = Set.empty,
    override val session: Map[String, String] = Map.empty,
    override val preparedStatement: Map[String, String] = Map.empty) extends TrinoContext(
    user,
    timeZone,
    clientCapabilities,
    source,
    catalog,
    schema,
    remoteUserAddress,
    language,
    traceToken,
    clientInfo,
    clientTags,
    session,
    preparedStatement) {}

object PrestoContext {

  private val PRESTO_HEADERS = ProtocolHeaders.createProtocolHeaders("Presto")

  def apply(headers: HttpHeaders, remoteAddress: Option[String]): TrinoContext = {
    val context = apply(headers.getRequestHeaders.asScala.toMap.map {
      case (k, v) => (k, v.asScala.toList)
    })
    context.copy(remoteUserAddress = remoteAddress)
  }

  def apply(headers: Map[String, List[String]]): TrinoContext = {
    val requestCtx = TrinoContext("")
    val kvPattern = """(.+)=(.+)""".r
    headers.foldLeft(requestCtx) { case (context, (k, v)) =>
      k match {
        case k if PRESTO_HEADERS.requestUser.equalsIgnoreCase(k) && v.nonEmpty =>
          context.copy(user = v.head)
        case k if PRESTO_HEADERS.requestTimeZone.equalsIgnoreCase(k) =>
          context.copy(timeZone = v.headOption)
        case k if PRESTO_HEADERS.requestClientCapabilities.equalsIgnoreCase(k) =>
          context.copy(clientCapabilities = v.headOption)
        case k if PRESTO_HEADERS.requestSource.equalsIgnoreCase(k) =>
          context.copy(source = v.headOption)
        case k if PRESTO_HEADERS.requestCatalog.equalsIgnoreCase(k) =>
          context.copy(catalog = v.headOption)
        case k if PRESTO_HEADERS.requestSchema.equalsIgnoreCase(k) =>
          context.copy(schema = v.headOption)
        case k if PRESTO_HEADERS.requestLanguage.equalsIgnoreCase(k) =>
          context.copy(language = v.headOption)
        case k if PRESTO_HEADERS.requestTraceToken.equalsIgnoreCase(k) =>
          context.copy(traceToken = v.headOption)
        case k if PRESTO_HEADERS.requestClientInfo.equalsIgnoreCase(k) =>
          context.copy(clientInfo = v.headOption)
        case k if PRESTO_HEADERS.requestClientTags.equalsIgnoreCase(k) && v.nonEmpty =>
          context.copy(clientTags = v.head.split(",").toSet)
        case k if PRESTO_HEADERS.requestSession.equalsIgnoreCase(k) =>
          val session = v.collect {
            case kvPattern(key, value) => (key, urlDecode(value))
          }.toMap
          context.copy(session = session)
        case k if PRESTO_HEADERS.requestPreparedStatement.equalsIgnoreCase(k) =>
          val preparedStatement = v.collect {
            case kvPattern(key, value) => (key, urlDecode(value))
          }.toMap
          context.copy(preparedStatement = preparedStatement)

        case k
            if PRESTO_HEADERS.requestTransactionId.equalsIgnoreCase(k)
              && v.headOption.exists(_ != "NONE") =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if PRESTO_HEADERS.requestPath.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if PRESTO_HEADERS.requestRole.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if PRESTO_HEADERS.requestResourceEstimate.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if PRESTO_HEADERS.requestExtraCredential.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case k if PRESTO_HEADERS.requestRole.equalsIgnoreCase(k) =>
          throw new UnsupportedOperationException(s"$k is not currently supported")
        case _ =>
          context
      }
    }
  }

  def buildTrinoHeader(headers: HttpHeaders): Map[String, Seq[String]] = {
    headers.getRequestHeaders.asScala.toMap.map {
      case (k, v) =>
        k match {
          case k if PRESTO_HEADERS.requestUser.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestUser -> v.asScala
          case k if PRESTO_HEADERS.requestTimeZone.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestTimeZone -> v.asScala
          case k if PRESTO_HEADERS.requestClientCapabilities.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestClientCapabilities -> v.asScala
          case k if PRESTO_HEADERS.requestSource.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestSource -> v.asScala
          case k if PRESTO_HEADERS.requestCatalog.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestCatalog -> v.asScala
          case k if PRESTO_HEADERS.requestSchema.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestSchema -> v.asScala
          case k if PRESTO_HEADERS.requestLanguage.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestLanguage -> v.asScala
          case k if PRESTO_HEADERS.requestTraceToken.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestTraceToken -> v.asScala
          case k if PRESTO_HEADERS.requestClientInfo.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestClientInfo -> v.asScala
          case k if PRESTO_HEADERS.requestClientTags.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestClientTags -> v.asScala
          case k if PRESTO_HEADERS.requestSession.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestSession -> v.asScala
          case k if PRESTO_HEADERS.requestPreparedStatement.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestPreparedStatement -> v.asScala
          case k if PRESTO_HEADERS.requestTransactionId.equalsIgnoreCase(k) =>
            TRINO_HEADERS.requestTransactionId -> v.asScala
          case _ =>
            k -> v.asScala
        }
    }
  }
}
