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

package org.apache.kyuubi.util

import java.io.IOException
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import okhttp3._

import org.apache.kyuubi.Logging

object HttpUtil extends Logging {

  private val TIMEOUT = 300 // 300s

  private def buildHeaders(headers: Map[String, Seq[String]]): Headers = {
    val hb = new Headers.Builder
    headers.foreach {
      case (k, v) => v.foreach { v0 =>
          hb.add(k, v0)
        }
    }
    hb.build()
  }

  private def processResponse(resp: Response): (Map[String, Seq[String]], String) = {
    val respHeader = resp.headers().toMultimap.asScala.map {
      case (k, v) => k -> v.asScala
    }.toMap
    var compressed = false
    if (respHeader.contains("content-encoding")) {
      respHeader.getOrElse("content-encoding", Seq()).foreach { h =>
        if (h.equalsIgnoreCase("gzip")) {
          compressed = true
        }
      }
    }
    val respBody = resp.body()
    if (compressed) {
      (respHeader, new String(CompressionUtil.decompressByGzip(respBody.bytes())))
    } else {
      (respHeader, respBody.string())
    }
  }

  def post(
      url: String,
      headers: Map[String, Seq[String]],
      data: String): (Map[String, Seq[String]], String) = {
    val mt = headers.getOrElse("Content-Type", Seq(javax.ws.rs.core.MediaType.TEXT_PLAIN)).head
    val reqBody = RequestBody.create(data, MediaType.parse(mt))
    val req = new Request.Builder()
      .url(url)
      .post(reqBody)
      .headers(buildHeaders(headers))
      .build()
    debug(s"req: $req")
    val client = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(TIMEOUT, TimeUnit.SECONDS)
      .build
    var resp: Response = null
    try {
      resp = client.newCall(req).execute
      if (200 != resp.code) {
        val msg = s"HttpPost error response code: [${resp.code}] not 200, resp: [${resp.toString}]"
        error(msg)
        throw new IOException(msg)
      }
      processResponse(resp)
    } finally {
      if (resp != null) resp.close()
    }
  }

  def get(url: String, headers: Map[String, Seq[String]]): (Map[String, Seq[String]], String) = {
    val req = new Request.Builder()
      .url(url)
      .headers(buildHeaders(headers))
      .get
      .build
    val client = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(TIMEOUT, TimeUnit.SECONDS)
      .build
    var resp: Response = null
    try {
      resp = client.newCall(req).execute
      if (200 != resp.code) {
        val msg = s"HttpGet error response code: [${resp.code}] not 200, resp: [${resp.toString}]"
        error(msg)
        throw new IOException(msg)
      }
      processResponse(resp)
    } finally {
      if (resp != null) resp.close()
    }
  }

  def delete(url: String, headers: Map[String, Seq[String]]): (Map[String, Seq[String]], String) = {
    val req = new Request.Builder()
      .url(url)
      .headers(buildHeaders(headers))
      .delete
      .build
    val client: OkHttpClient = new OkHttpClient.Builder()
      .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(TIMEOUT, TimeUnit.SECONDS)
      .build
    var resp: Response = null
    try {
      resp = client.newCall(req).execute
      if (200 != resp.code) {
        val msg =
          s"HttpDelete error response code: [${resp.code}] not 200, resp: [${resp.toString}]"
        error(msg)
        throw new IOException(msg)
      }
      processResponse(resp)
    } finally {
      if (resp != null) resp.close()
    }
  }
}
