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

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObsObjectStorage extends BaseObjectStorage {

  private static final Logger LOG = LoggerFactory.getLogger(ObsObjectStorage.class);

  private final ObsClient obs;

  public ObsObjectStorage(String endpoint, String accessKey, String secretKey) {
    ObsConfiguration config = new ObsConfiguration();
    config.setSocketTimeout(30000);
    config.setConnectionTimeout(10000);
    config.setEndPoint(endpoint);
    obs = new ObsClient(accessKey, secretKey, config);
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file) {
    PutObjectResult res = obs.putObject(bucketName, objectKey, file);
    return res.getEtag();
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file, String contentType)
      throws IOException {
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType(contentType);
    metadata.setContentLength(file.length());
    PutObjectResult res = obs.putObject(bucketName, objectKey, file, metadata);
    return res.getEtag();
  }

  @Override
  InputStream getObjectInputStream(String bucketName, String objectKey) {
    ObsObject obj = obs.getObject(bucketName, objectKey, null);
    if (null == obj) {
      return null;
    }
    return obj.getObjectContent();
  }

  @Override
  public List<String> listObjects(String bucketName, String prefix) {
    String nextMarker = null;
    ListObjectsRequest req = new ListObjectsRequest(bucketName);
    req.setPrefix(prefix);
    List<String> res = new ArrayList<>();
    ObjectListing objectListing;
    do {
      req.setMarker(nextMarker);
      objectListing = obs.listObjects(req);
      for (ObsObject object : objectListing.getObjects()) {
        res.add(object.getObjectKey());
      }
      nextMarker = objectListing.getNextMarker();
    } while (objectListing.isTruncated());
    return res;
  }

  @Override
  public String signObject(String bucketName, String objectKey, Duration expireDuration) {
    TemporarySignatureRequest req =
        new TemporarySignatureRequest(HttpMethodEnum.GET, expireDuration.getSeconds());
    req.setBucketName(bucketName);
    req.setObjectKey(objectKey);
    // req.setSpecialParam(SpecialParamEnum.ACL);
    TemporarySignatureResponse res = obs.createTemporarySignature(req);
    return res.getSignedUrl();
  }

  @Override
  public long objectSize(String bucketName, String objectKey) {
    ObsObject obj = obs.getObject(bucketName, objectKey);
    if (null == obj) {
      return 0L;
    }
    ObjectMetadata meta = obj.getMetadata();
    if (null == meta) {
      return 0L;
    }
    return meta.getContentLength();
  }

  @Override
  public long objectsSize(String bucketName, String prefix, String suffix) {
    String nextMarker = null;
    ListObjectsRequest req = new ListObjectsRequest(bucketName);
    req.setPrefix(prefix);
    long res = 0L;
    ObjectListing objectListing;
    do {
      req.setMarker(nextMarker);
      objectListing = obs.listObjects(req);
      for (ObsObject obj : objectListing.getObjects()) {
        if (!StringUtils.isEmpty(suffix) && !obj.getObjectKey().endsWith(suffix)) {
          continue;
        }
        ObjectMetadata meta = obj.getMetadata();
        if (null == meta) {
          continue;
        }
        res += meta.getContentLength();
      }
      nextMarker = objectListing.getNextMarker();
    } while (objectListing.isTruncated());
    return res;
  }

  @Override
  public String deleteObject(String bucketName, String objectKey) {
    DeleteObjectResult res = obs.deleteObject(bucketName, objectKey);
    if (res.getStatusCode() - HttpStatus.SC_OK > 100) {
      LOG.error("deleteObject failed retCode [{}] not 200", res.getStatusCode());
    }
    return objectKey;
  }

  @Override
  public void close() {
    if (null != obs) {
      try {
        obs.close();
      } catch (IOException e) {
        LOG.error("close obsClient failed", e);
      }
    }
  }
}
