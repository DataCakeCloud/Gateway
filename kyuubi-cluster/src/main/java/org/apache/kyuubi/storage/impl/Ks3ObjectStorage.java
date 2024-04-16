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

import com.ksyun.ks3.dto.*;
import com.ksyun.ks3.service.Ks3;
import com.ksyun.ks3.service.Ks3Client;
import com.ksyun.ks3.service.Ks3ClientConfig;
import com.ksyun.ks3.service.request.GetObjectRequest;
import com.ksyun.ks3.service.request.ListObjectsRequest;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ks3ObjectStorage extends BaseObjectStorage {

  private static final Logger LOG = LoggerFactory.getLogger(Ks3ObjectStorage.class);
  private final Ks3 ks3;

  public Ks3ObjectStorage(String endpoint, String accessKey, String secretKey) {
    Ks3ClientConfig config = new Ks3ClientConfig();
    config.setEndpoint(endpoint);
    config.setPathStyleAccess(false);
    ks3 = new Ks3Client(accessKey, secretKey, config);
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file) {
    PutObjectResult res = ks3.putObject(bucketName, objectKey, file);
    return res.geteTag();
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file, String contentType)
      throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(contentType);
    metadata.setContentLength(file.length());
    PutObjectResult res =
        ks3.putObject(bucketName, objectKey, Files.newInputStream(file.toPath()), metadata);
    return res.geteTag();
  }

  @Override
  InputStream getObjectInputStream(String bucketName, String objectKey) {
    GetObjectRequest req = new GetObjectRequest(bucketName, objectKey);
    Ks3Object object = ks3.getObject(req).getObject();
    if (null == object) {
      return null;
    }
    return object.getObjectContent();
  }

  public List<String> listObjects(String bucketName, String prefix) {
    String nextMarker = null;
    ListObjectsRequest req = new ListObjectsRequest(bucketName);
    req.setPrefix(prefix);
    List<String> res = new ArrayList<>();
    ObjectListing objectListing;
    do {
      req.setMarker(nextMarker);
      objectListing = ks3.listObjects(req);
      for (Ks3ObjectSummary object : objectListing.getObjectSummaries()) {
        res.add(object.getKey());
      }
      nextMarker = objectListing.getNextMarker();
    } while (objectListing.isTruncated());
    return res;
  }

  public String signObject(String bucketName, String objectKey, Duration expireDuration) {
    return ks3.generatePresignedUrl(bucketName, objectKey, (int) expireDuration.getSeconds());
  }

  public String deleteObject(String bucketName, String objectKey) {
    ks3.deleteObject(bucketName, objectKey);
    return objectKey;
  }

  public long objectSize(String bucketName, String objectKey) {
    GetObjectResult res = ks3.getObject(bucketName, objectKey);
    return res.getObject().getObjectMetadata().getContentLength();
  }

  public long objectsSize(String bucketName, String prefix, String suffix) {
    long res = 0;
    String nextMarker = null;
    ListObjectsRequest req = new ListObjectsRequest(bucketName);
    ks3.listObjects(bucketName, prefix);
    req.setPrefix(prefix);
    ObjectListing objectListing;
    do {
      req.setMarker(nextMarker);
      objectListing = ks3.listObjects(req);
      for (Ks3ObjectSummary object : objectListing.getObjectSummaries()) {
        res += object.getSize();
      }
      nextMarker = objectListing.getNextMarker();
    } while (objectListing.isTruncated());
    return res;
  }
}
