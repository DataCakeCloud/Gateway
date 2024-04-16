/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.client.api.v1.dto;

import java.util.*;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SignatureUrlResponse {

  public static final SignatureUrlResponse EMPTY =
      new SignatureUrlResponse(true, "SUCCESS", Collections.emptyMap());

  private boolean success = false;
  private String msg = null;
  private Map<String, List<String>> urls;

  public SignatureUrlResponse() {}

  public SignatureUrlResponse(boolean success, String msg) {
    this.success = success;
    this.msg = msg;
    this.urls = new HashMap<>();
  }

  public SignatureUrlResponse(boolean success, String msg, Map<String, List<String>> urls) {
    this.success = success;
    this.msg = msg;
    this.urls = urls;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  public Map<String, List<String>> getUrls() {
    return urls;
  }

  public void setUrls(Map<String, List<String>> urls) {
    this.urls = urls;
  }

  public void putUrl(String operationId, String url) {
    if (null == urls) {
      urls = new HashMap<>();
    }
    urls.computeIfAbsent(operationId, k -> new ArrayList<>());
    urls.get(operationId).add(url);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SignatureUrlResponse that = (SignatureUrlResponse) o;
    return success == that.success
        && Objects.equals(msg, that.msg)
        && Objects.equals(urls, that.urls);
  }

  @Override
  public int hashCode() {
    return Objects.hash(success, msg, urls);
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
