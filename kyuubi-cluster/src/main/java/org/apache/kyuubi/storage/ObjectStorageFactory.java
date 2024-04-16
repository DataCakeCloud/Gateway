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

import com.aliyuncs.exceptions.ClientException;
import org.apache.kyuubi.storage.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStorageFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectStorageFactory.class);

  public static ObjectStorage create(ObjectStorageType type, String regionOrEndpoint)
      throws Exception {
    ObjectStorage os;
    switch (type) {
      case S3:
        os = new S3ObjectStorage(regionOrEndpoint);
        break;
      case GS:
        os = new GsObjectStorage();
        break;
      case OSS:
        os = new OssObjectStorage(regionOrEndpoint);
        break;
      default:
        LOG.error("Unsupported type to create ObjectStorage:[{}]", type);
        throw new IllegalArgumentException("unsupported protocol to create ObjectStorage");
    }
    return os;
  }

  public static ObjectStorage create(
      ObjectStorageType type, String endpoint, String accessKey, String secretKey) {
    ObjectStorage os;
    switch (type) {
      case OBS:
        os = new ObsObjectStorage(endpoint, accessKey, secretKey);
        break;
      case KS3:
        os = new Ks3ObjectStorage(endpoint, accessKey, secretKey);
        break;
      case OSS:
        try {
          os = new OssObjectStorage(endpoint, accessKey, secretKey);
        } catch (ClientException e) {
          LOG.error("Init OssObjectStorage failed", e);
          throw new RuntimeException(e);
        }
        break;
      default:
        LOG.error("Unsupported type to create ObjectStorage:[{}]", type);
        throw new IllegalArgumentException("unsupported protocol to create ObjectStorage");
    }
    return os;
  }
}
