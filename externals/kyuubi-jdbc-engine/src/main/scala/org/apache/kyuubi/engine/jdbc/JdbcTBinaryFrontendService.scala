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
package org.apache.kyuubi.engine.jdbc

import java.util.Locale

import org.apache.hive.service.rpc.thrift.{TFetchResultsReq, TFetchResultsResp}

import org.apache.kyuubi.config.KyuubiConf.ENGINE_JDBC_CONNECTION_PROVIDER
import org.apache.kyuubi.engine.jdbc.operation.ExecuteStatement
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.operation.FetchOrientation.FETCH_NEXT
import org.apache.kyuubi.operation.OperationHandle
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.OK_STATUS

class JdbcTBinaryFrontendService(override val serverable: Serverable)
  extends TBinaryFrontendService("JdbcTBinaryFrontend") {

  /**
   * An optional `ServiceDiscovery` for [[FrontendService]] to expose itself
   */
  override lazy val discoveryService: Option[Service] =
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new EngineServiceDiscovery(this))
    } else {
      None
    }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    val operationHandle = OperationHandle(req.getOperationHandle)
    val operation = be.sessionManager.operationManager.getOperation(operationHandle)
      .asInstanceOf[ExecuteStatement]
    val session = operation.getSession
    val provider =
      session.conf.getOrElse(ENGINE_JDBC_CONNECTION_PROVIDER.key, "").toUpperCase(Locale.ROOT)
    if (provider.startsWith("HIVE")) {
      val fetchLog = req.getFetchType == 1
      if (fetchLog) {
        val rowSet = operation.getQueryLog(FETCH_NEXT, KyuubiStatement.DEFAULT_FETCH_SIZE)
        val resp = new TFetchResultsResp
        resp.setResults(rowSet)
        resp.setStatus(OK_STATUS)
        resp
      } else {
        super.FetchResults(req)
      }
    } else {
      super.FetchResults(req)
    }
  }
}
