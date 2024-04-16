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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class S3ObjectStorage extends BaseObjectStorage {

  private final AmazonS3 s3;

  public S3ObjectStorage(String region) {
    s3 = AmazonS3ClientBuilder.defaultClient();
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file) {
    PutObjectResult result = s3.putObject(bucketName, objectKey, file);
    return result.getETag();
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file, String contentType)
      throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(contentType);
    metadata.setContentLength(file.length());
    PutObjectResult result =
        s3.putObject(bucketName, objectKey, Files.newInputStream(file.toPath()), metadata);
    return result.getETag();
  }

  @Override
  InputStream getObjectInputStream(String bucketName, String objectKey) {
    S3Object obj = s3.getObject(bucketName, objectKey);
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
      objectListing = s3.listObjects(req);
      for (S3ObjectSummary object : objectListing.getObjectSummaries()) {
        res.add(object.getKey());
      }
      nextMarker = objectListing.getNextMarker();
    } while (objectListing.isTruncated());
    return res;
  }

  @Override
  public String signObject(String bucketName, String prefix, Duration expireDuration) {
    URL url =
        s3.generatePresignedUrl(
            bucketName,
            prefix,
            new Date(System.currentTimeMillis() + expireDuration.getSeconds() * 1000L));
    return url.toString();
  }

  @Override
  public long objectSize(String bucketName, String objectKey) {
    S3Object obj = s3.getObject(bucketName, objectKey);
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
      objectListing = s3.listObjects(req);
      for (S3ObjectSummary obj : objectListing.getObjectSummaries()) {
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
    s3.deleteObject(bucketName, objectKey);
    return objectKey;
  }
}
