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

package org.apache.kyuubi.server

import org.apache.hive.service.rpc.thrift.TOpenSessionReq
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.TClientTestUtils

class KyuubiTBinaryFrontendServiceSuite extends WithKyuubiServer with KyuubiFunSuite {

  override protected val conf: KyuubiConf = KyuubiConf()

  test("connection metrics") {
    val totalConnections =
      MetricsSystem.counterValue(MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L)
    val openConnections =
      MetricsSystem.counterValue(MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L)

    TClientTestUtils.withThriftClient(server.frontendServices.head) {
      client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        client.OpenSession(req)

        assert(MetricsSystem.counterValue(
          MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections === 1)
        assert(MetricsSystem.counterValue(
          MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections === 1)

        TClientTestUtils.withThriftClient(server.frontendServices.head) {
          client =>
            val req = new TOpenSessionReq()
            req.setUsername(Utils.currentUser)
            req.setPassword("anonymous")
            client.OpenSession(req)

            assert(MetricsSystem.counterValue(
              MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections
              === 2)
            assert(MetricsSystem.counterValue(
              MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections
              === 2)
        }

        eventually(timeout(1.seconds), interval(200.milliseconds)) {
          assert(MetricsSystem.counterValue(
            MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections
            === 2)
          assert(MetricsSystem.counterValue(
            MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections === 1)
        }
    }

    eventually(timeout(1.seconds), interval(200.milliseconds)) {
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_BINARY_CONN_TOTAL).getOrElse(0L) - totalConnections
        === 2)
      assert(MetricsSystem.counterValue(
        MetricsConstants.THRIFT_BINARY_CONN_OPEN).getOrElse(0L) - openConnections === 0)
    }
  }
}
