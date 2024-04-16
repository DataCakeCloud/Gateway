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

package org.apache.kyuubi.storage.impl;

import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;
import com.aliyun.oss.internal.OSSHeaders;
import com.aliyun.oss.model.*;
import com.aliyuncs.exceptions.ClientException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class OssObjectStorage extends BaseObjectStorage {

  private final OSS ossClient;

  public OssObjectStorage(String endpoint) throws ClientException {
    EnvironmentVariableCredentialsProvider credentialsProvider =
        CredentialsProviderFactory.newEnvironmentVariableCredentialsProvider();
    ossClient = new OSSClientBuilder().build(endpoint, credentialsProvider);
  }

  public OssObjectStorage(String endpoint, String accessId, String accessSecret)
      throws ClientException {
    if (StringUtils.isBlank(accessId)) {
      EnvironmentVariableCredentialsProvider credentialsProvider =
          CredentialsProviderFactory.newEnvironmentVariableCredentialsProvider();
      ossClient = new OSSClientBuilder().build(endpoint, credentialsProvider);
    } else {
      ossClient = new OSSClientBuilder().build(endpoint, accessId, accessSecret);
    }
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file) throws IOException {
    PutObjectRequest req = new PutObjectRequest(bucketName, objectKey, file);
    PutObjectResult result = ossClient.putObject(req);
    return result.getETag();
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file, String contentType)
      throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setHeader(OSSHeaders.CONTENT_TYPE, contentType);
    PutObjectRequest req = new PutObjectRequest(bucketName, objectKey, file);
    req.setMetadata(metadata);
    PutObjectResult result = ossClient.putObject(req);
    return result.getETag();
  }

  @Override
  InputStream getObjectInputStream(String bucketName, String objectKey) throws IOException {
    OSSObject obj = ossClient.getObject(bucketName, objectKey);
    if (null == obj) {
      return null;
    }
    return obj.getObjectContent();
  }

  @Override
  public List<String> listObjects(String bucketName, String prefix) {
    String nextMarker = null;
    ListObjectsRequest req = new ListObjectsRequest();
    req.setBucketName(bucketName);
    req.setPrefix(prefix);
    List<String> res = new ArrayList<>();
    ObjectListing objectListing;
    do {
      req.setMarker(nextMarker);
      objectListing = ossClient.listObjects(req);
      for (OSSObjectSummary object : objectListing.getObjectSummaries()) {
        res.add(object.getKey());
      }
      nextMarker = objectListing.getNextMarker();
    } while (objectListing.isTruncated());
    return res;
  }

  @Override
  public String signObject(String bucketName, String objectKey, Duration expireDuration) {
    GeneratePresignedUrlRequest req = new GeneratePresignedUrlRequest(bucketName, objectKey);
    req.setMethod(HttpMethod.GET);
    Date expiration = new Date(System.currentTimeMillis() + expireDuration.getSeconds() * 1000L);
    req.setExpiration(expiration);
    URL url = ossClient.generatePresignedUrl(req);
    return url.toString();
  }

  @Override
  public long objectSize(String bucketName, String objectKey) {
    OSSObject obj = ossClient.getObject(bucketName, objectKey);
    if (null == obj) {
      return 0L;
    }
    ObjectMetadata meta = obj.getObjectMetadata();
    if (null == meta) {
      return 0L;
    }

    return meta.getContentLength();
  }

  @Override
  public long objectsSize(String bucketName, String prefix, String suffix) {
    String nextMarker = null;
    ListObjectsRequest req = new ListObjectsRequest();
    req.setBucketName(bucketName);
    req.setPrefix(prefix);
    ObjectListing objectListing;
    long res = 0L;
    do {
      req.setMarker(nextMarker);
      objectListing = ossClient.listObjects(req);
      for (OSSObjectSummary obj : objectListing.getObjectSummaries()) {
        if (!StringUtils.isEmpty(suffix) && !obj.getKey().endsWith(suffix)) {
          continue;
        }
        res += obj.getSize();
      }
      nextMarker = objectListing.getNextMarker();
    } while (objectListing.isTruncated());
    return res;
  }

  @Override
  public String deleteObject(String bucketName, String objectKey) {
    ossClient.deleteObject(bucketName, objectKey);
    return objectKey;
  }

  @Override
  public void close() {
    if (null != ossClient) {
      ossClient.shutdown();
    }
  }
}
