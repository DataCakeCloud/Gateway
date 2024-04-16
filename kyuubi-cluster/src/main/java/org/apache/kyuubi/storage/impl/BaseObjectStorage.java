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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.storage.ObjectStorage;
import org.apache.kyuubi.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseObjectStorage implements ObjectStorage {

  private static final Logger LOG = LoggerFactory.getLogger(BaseObjectStorage.class);

  @Override
  public abstract String putObject(String bucketName, String objectKey, File file)
      throws IOException;

  @Override
  public abstract String putObject(
      String bucketName, String objectKey, File file, String contentType) throws IOException;

  abstract InputStream getObjectInputStream(String bucketName, String objectKey) throws IOException;

  @Override
  public File getObject(String bucketName, String objectKey, String destFilePath)
      throws IOException {
    InputStream is = getObjectInputStream(bucketName, objectKey);
    if (null == is) {
      return null;
    }
    FileUtils.copy(is, Files.newOutputStream(Paths.get(destFilePath)));
    return new File(destFilePath);
  }

  @Override
  public abstract List<String> listObjects(String bucketName, String prefix);

  @Override
  public List<String> listObjects(String bucketName, String prefix, String suffix) {
    if (StringUtils.isEmpty(suffix)) {
      return listObjects(bucketName, prefix);
    } else {
      return listObjects(bucketName, prefix).stream()
          .filter(a -> a.endsWith(suffix))
          .collect(Collectors.toList());
    }
  }

  @Override
  public abstract String signObject(String bucketName, String objectKey, Duration expireDuration);

  @Override
  public abstract long objectSize(String bucketName, String objectKey);

  @Override
  public abstract String deleteObject(String bucketName, String objectKey);

  @Override
  public List<String> signObjects(
      String bucketName, String prefix, String suffix, Duration expireDuration) {
    List<String> objects = listObjects(bucketName, prefix, suffix);
    List<String> result = new ArrayList<>();
    for (String obj : objects) {
      String url = signObject(bucketName, obj, expireDuration);
      result.add(url);
    }
    return result;
  }

  @Override
  public long objectsSize(String bucketName, String prefix, String suffix) {
    long res = 0L;
    List<String> objects = listObjects(bucketName, prefix, suffix);
    for (String obj : objects) {
      res += objectSize(bucketName, obj);
    }
    return res;
  }

  @Override
  public void deletePath(String bucketName, String prefix) {
    List<String> objects = listObjects(bucketName, prefix);
    for (String object : objects) {
      deleteObject(bucketName, object);
    }
  }

  @Override
  public void close() {}
}
