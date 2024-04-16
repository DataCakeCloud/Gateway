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

package org.apache.kyuubi.storage;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

public interface ObjectStorage extends AutoCloseable {

  String putObject(String bucketName, String objectKey, File file) throws IOException;

  String putObject(String bucketName, String objectKey, File file, String contentType)
      throws IOException;

  File getObject(String bucketName, String objectKey, String destFilePath) throws IOException;

  List<String> listObjects(String bucketName, String prefix);

  List<String> listObjects(String bucketName, String prefix, String suffix);

  String signObject(String bucketName, String objectKey, Duration expireDuration);

  List<String> signObjects(
      String bucketName, String prefix, String suffix, Duration expireDuration);

  String deleteObject(String bucketName, String objectKey);

  long objectSize(String bucketName, String objectKey);

  long objectsSize(String bucketName, String prefix, String suffix);

  void deletePath(String bucketName, String prefix);

  void close();
}
