/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.kyuubi.util;
//
// import okhttp3.*;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// import java.io.IOException;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.concurrent.TimeUnit;
//
// public class HttpUtil {
//
//  private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);
//  private static final int TIMEOUT = 300; // 300s
//
//  public static String post(String url,
//                            Map<String, String> headers,
//                            String data) throws IOException {
//    if (null == headers) {
//      headers = new HashMap<>();
//    }
//    String mt = headers.getOrDefault("Content-Type", javax.ws.rs.core.MediaType.TEXT_PLAIN);
//    RequestBody reqBody = RequestBody.create(data, MediaType.parse(mt));
//    LOG.debug("HttpUtil post header: {}", Headers.of(headers));
//    Request req = new Request.Builder()
//        .url(url)
//        .headers(Headers.of(headers))
//        .post(reqBody)
//        .build();
//    OkHttpClient client = new OkHttpClient.Builder()
//        .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .readTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .build();
//    try (Response resp = client.newCall(req).execute()) {
//      if (200 != resp.code()) {
//        String msg = String.format("HttpPost error response code: [%d] not 200", resp.code());
//        LOG.error(msg);
//        throw new IOException(msg);
//      }
//      ResponseBody respBody = resp.body();
//      if (null == respBody) {
//        String msg = "HttpPost error response body is null";
//        LOG.error(msg);
//        throw new IOException(msg);
//      }
//      return respBody.string();
//    }
//  }
//
//  public static String get(String url, Map<String, String> headers) throws IOException {
//    if (null == headers) {
//      headers = new HashMap<>();
//    }
//    Request req = new Request.Builder()
//        .url(url)
//        .headers(Headers.of(headers))
//        .get()
//        .build();
//    OkHttpClient client = new OkHttpClient.Builder()
//        .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .readTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .build();
//    try (Response resp = client.newCall(req).execute()) {
//      if (200 != resp.code()) {
//        String msg = String.format("HttpPost error response code: [%d] not 200", resp.code());
//        LOG.error(msg);
//        throw new IOException(msg);
//      }
//      ResponseBody respBody = resp.body();
//      if (null == respBody) {
//        String msg = "HttpPost error response body is null";
//        LOG.error(msg);
//        throw new IOException(msg);
//      }
//      return respBody.string();
//    }
//  }
//
//  public static String delete(String url, Map<String, String> headers) throws IOException {
//    if (null == headers) {
//      headers = new HashMap<>();
//    }
//    Request req = new Request.Builder()
//        .url(url)
//        .headers(Headers.of(headers))
//        .delete()
//        .build();
//    OkHttpClient client = new OkHttpClient.Builder()
//        .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .readTimeout(TIMEOUT, TimeUnit.SECONDS)
//        .build();
//    try (Response resp = client.newCall(req).execute()) {
//      if (200 != resp.code()) {
//        String msg = String.format("HttpPost error response code: [%d] not 200", resp.code());
//        LOG.error(msg);
//        throw new IOException(msg);
//      }
//      ResponseBody respBody = resp.body();
//      if (null == respBody) {
//        String msg = "HttpPost error response body is null";
//        LOG.error(msg);
//        throw new IOException(msg);
//      }
//      return respBody.string();
//    }
//  }
// }
