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

import java.util.Base64

import org.apache.kyuubi.client.{BatchRestApi, KyuubiRestClient}
import org.apache.kyuubi.client.api.v1.dto.{Batch, CloseBatchResponse, OperationLog}
import org.apache.kyuubi.client.auth.AuthHeaderGenerator
import org.apache.kyuubi.server.http.authentication.AuthSchemes
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

/**
 * The internal rest client for request redirection and it is shared for all the requests
 * redirected to the same Kyuubi instance.
 *
 * @param kyuubiInstance the kyuubi instance host:port.
 * @param socketTimeout the socket timeout for http client.
 * @param connectTimeout the connect timeout for http client.
 */
class InternalRestClient(kyuubiInstance: String, socketTimeout: Int, connectTimeout: Int) {
  require(
    InternalSecurityAccessor.get() != null,
    "Internal secure access across Kyuubi instances is not enabled")

  private val internalBatchRestApi = new BatchRestApi(initKyuubiRestClient())

  def getBatch(user: String, batchId: String): Batch = {
    withAuthUser(user) {
      internalBatchRestApi.getBatchById(batchId)
    }
  }

  def getBatchLocalLog(user: String, batchId: String, from: Int, size: Int): OperationLog = {
    withAuthUser(user) {
      internalBatchRestApi.getBatchLocalLog(batchId, from, size)
    }
  }

  def deleteBatch(user: String, batchId: String): CloseBatchResponse = {
    withAuthUser(user) {
      internalBatchRestApi.deleteBatch(batchId, null)
    }
  }

  def deleteInteractive(user: String, sessionId: String): CloseBatchResponse = {
    withAuthUser(user) {
      internalBatchRestApi.deleteInteractive(sessionId, null)
    }
  }

  private def initKyuubiRestClient(): KyuubiRestClient = {
    KyuubiRestClient.builder(s"http://$kyuubiInstance")
      .apiVersion(KyuubiRestClient.ApiVersion.V1)
      .socketTimeout(socketTimeout)
      .connectionTimeout(connectTimeout)
      .authHeaderGenerator(InternalRestClient.internalAuthHeaderGenerator)
      .build()
  }

  private def withAuthUser[T](user: String)(f: => T): T = {
    try {
      InternalRestClient.AUTH_USER.set(user)
      f
    } finally {
      InternalRestClient.AUTH_USER.remove()
    }
  }
}

object InternalRestClient {
  final val AUTH_USER = new ThreadLocal[String]() {
    override def initialValue(): String = null
  }

  final val internalAuthHeaderGenerator = new AuthHeaderGenerator {
    override def generateAuthHeader(): String = {
      val authUser = AUTH_USER.get()
      require(authUser != null, "The auth user shall be not null")
      val encodedAuthorization = new String(
        Base64.getEncoder.encode(
          s"$authUser:${InternalSecurityAccessor.get().issueToken()}".getBytes()),
        "UTF-8")
      s"${AuthSchemes.KYUUBI_INTERNAL.toString} $encodedAuthorization"
    }
  }
}
