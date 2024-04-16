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

package org.apache.kyuubi.server.http.authentication.trino.api

import java.io.IOException
import javax.security.sasl.AuthenticationException
import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.mutable

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER
import org.apache.kyuubi.server.http.authentication.{AuthenticationAuditLogger, AuthenticationHandler, BasicAuthenticationHandler}
import org.apache.kyuubi.server.http.authentication.AuthenticationFilter.{HTTP_AUTH_TYPE, HTTP_CLIENT_IP_ADDRESS, HTTP_CLIENT_USER_NAME, HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS}
import org.apache.kyuubi.server.http.authentication.AuthenticationHandler.AUTHORIZATION_HEADER
import org.apache.kyuubi.server.http.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.service.authentication.AuthTypes

class TrinoAuthenticationFilter(conf: KyuubiConf) extends Filter with Logging {

  private[authentication] val authSchemeHandlers =
    new mutable.HashMap[AuthScheme, AuthenticationHandler]()

  private[authentication] def addAuthHandler(authHandler: AuthenticationHandler): Unit = {
    authHandler.init(conf)
    if (authHandler.authenticationSupported) {
      if (authSchemeHandlers.contains(authHandler.authScheme)) {
        warn(s"Authentication handler has been defined for scheme ${authHandler.authScheme}")
      } else {
        info(s"Add authentication handler ${authHandler.getClass.getSimpleName}" +
          s" for scheme ${authHandler.authScheme}")
        authSchemeHandlers.put(authHandler.authScheme, authHandler)
      }
    } else {
      warn(s"The authentication handler ${authHandler.getClass.getSimpleName}" +
        s" for scheme ${authHandler.authScheme} is not supported")
    }
  }

  private[kyuubi] def initAuthHandlers(): Unit = {
    val basicHandler = new BasicAuthenticationHandler(AuthTypes.LDAP)
    addAuthHandler(basicHandler)
  }

  override def init(filterConfig: FilterConfig): Unit = {
    initAuthHandlers()
    super.init(filterConfig)
  }

  private[kyuubi] def getMatchedHandler(authorization: String): Option[AuthenticationHandler] = {
    authSchemeHandlers.values.find(_.matchAuthScheme(authorization))
  }

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      filterChain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    val authorization = httpRequest.getHeader(AUTHORIZATION_HEADER)
    info(s"Trino request Authorization: [$authorization]")
    val matchedHandler = getMatchedHandler(authorization).orNull
    HTTP_CLIENT_IP_ADDRESS.set(httpRequest.getRemoteAddr)
    HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.set(
      httpRequest.getHeader(conf.get(FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER)))

    if (matchedHandler == null) {
      debug(s"No auth scheme matched for url: ${httpRequest.getRequestURL}")
      httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
      AuthenticationAuditLogger.audit(httpRequest, httpResponse)
      httpResponse.sendError(
        HttpServletResponse.SC_UNAUTHORIZED,
        s"No auth scheme matched for $authorization")
    } else {
      HTTP_AUTH_TYPE.set(matchedHandler.authScheme.toString)
      try {
        val authUser = matchedHandler.authenticate(httpRequest, httpResponse)
        if (authUser != null) {
          HTTP_CLIENT_USER_NAME.set(authUser)
          doFilter(filterChain, httpRequest, httpResponse)
        }
        AuthenticationAuditLogger.audit(httpRequest, httpResponse)
      } catch {
        case e: AuthenticationException =>
          httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN)
          AuthenticationAuditLogger.audit(httpRequest, httpResponse)
          HTTP_CLIENT_USER_NAME.remove()
          HTTP_CLIENT_IP_ADDRESS.remove()
          HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.remove()
          HTTP_AUTH_TYPE.remove()
          httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage)
      }
    }
  }

  @throws[IOException]
  @throws[ServletException]
  protected def doFilter(
      filterChain: FilterChain,
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    filterChain.doFilter(request, response)
  }

  override def destroy(): Unit = {
    if (authSchemeHandlers.nonEmpty) {
      authSchemeHandlers.values.foreach(_.destroy())
      authSchemeHandlers.clear()
    }
  }
}
