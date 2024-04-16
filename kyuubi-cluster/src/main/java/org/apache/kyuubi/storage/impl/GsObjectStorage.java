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

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GsObjectStorage extends BaseObjectStorage {

  private static final Logger LOG = LoggerFactory.getLogger(GsObjectStorage.class);
  private final Storage storage;

  public GsObjectStorage() throws IOException {
    storage =
        StorageOptions.newBuilder()
            .setCredentials(ServiceAccountCredentials.getApplicationDefault())
            .build()
            .getService();
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file) throws IOException {
    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, objectKey)).build();
    Blob blob = storage.createFrom(blobInfo, file.toPath());
    return blob.getEtag();
  }

  @Override
  public String putObject(String bucketName, String objectKey, File file, String contentType)
      throws IOException {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("ContentType", contentType);
    BlobInfo blobInfo =
        BlobInfo.newBuilder(BlobId.of(bucketName, objectKey)).setMetadata(metadata).build();
    Blob blob = storage.createFrom(blobInfo, file.toPath());
    return blob.getEtag();
  }

  @Override
  InputStream getObjectInputStream(String bucketName, String objectKey) throws IOException {
    return null;
  }

  @Override
  public File getObject(String bucketName, String objectKey, String destFilePath)
      throws IOException {
    Blob blob = storage.get(BlobId.of(bucketName, objectKey));
    Path path = Paths.get(destFilePath);
    blob.downloadTo(path);
    return path.toFile();
  }

  @Override
  public List<String> listObjects(String bucketName, String prefix) {
    Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(prefix));
    List<String> result = new ArrayList<>();
    for (Blob blob : blobs.iterateAll()) {
      result.add(blob.getName());
    }
    return result;
  }

  @Override
  public String signObject(String bucketName, String objectKey, Duration expireDuration) {
    Blob blob = storage.get(BlobId.of(bucketName, objectKey));
    return storage.signUrl(blob, expireDuration.getSeconds(), TimeUnit.SECONDS).toString();
  }

  @Override
  public long objectSize(String bucketName, String objectKey) {
    Blob blob = storage.get(BlobId.of(bucketName, objectKey));
    return blob.getSize();
  }

  @Override
  public String deleteObject(String bucketName, String objectKey) {
    if (!storage.delete(bucketName, objectKey)) {
      LOG.error("GsStorage delete object [{}] failed", objectKey);
    }
    return objectKey;
  }
}
