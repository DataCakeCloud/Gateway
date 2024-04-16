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

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStorageUrlParser {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectStorageUrlParser.class);

  private ObjectStorageType storageType = ObjectStorageType.UNKNOWN;
  private String bucketName = "";
  private String prefix = "";
  private String objectKey = "";
  private String filename = "";

  public static ObjectStorageUrlParser parser(String url) {
    return parser(url, false);
  }

  public static ObjectStorageUrlParser parser(String url, boolean isFile) {
    if (StringUtils.isEmpty(url)) {
      LOG.error("Create ObjectStorage failed output is null");
      throw new IllegalArgumentException("url is null");
    }
    ObjectStorageUrlParser parser = new ObjectStorageUrlParser();
    if (!url.contains("://")) {
      parser.setStorageType(ObjectStorageType.FILE);
      parser.setObjectKey(url);
      if (isFile) {
        Path p = Paths.get(url);
        Path parent = p.getParent();
        if (null != parent) {
          parser.setPrefix(parent.toString());
        } else {
          parser.setPrefix("");
        }
        parser.setFilename(p.getFileName().toString());
      } else {
        parser.setPrefix(url);
      }
    } else {
      String[] ss = url.split("://");
      if (ss.length != 2) {
        LOG.error("url format failed:[{}]", url);
        throw new IllegalArgumentException("url format failed");
      }
      String protocol = ss[0];
      String uri = ss[1];
      ObjectStorageType type = ObjectStorageType.fromName(protocol);
      switch (type) {
        case FILE:
        case LOCAL:
          parser.setStorageType(ObjectStorageType.FILE);
          parser.setObjectKey(url);
          if (isFile) {
            Path p = Paths.get(url);
            Path parent = p.getParent();
            if (null != parent) {
              parser.setPrefix(parent.toString());
            } else {
              parser.setPrefix("");
            }
            parser.setFilename(p.getFileName().toString());
          } else {
            parser.setPrefix(url);
          }
          break;
        case OBS:
        case KS3:
        case S3:
        case GS:
        case OSS:
          parser.setStorageType(type);
          int idx = uri.indexOf("/");
          if (idx == 0) {
            LOG.error("url format failed bucketName not found:[{}]", url);
            throw new IllegalArgumentException(
                String.format("url format failed bucketName not found:[%s]", uri));
          }
          if (idx < 0) {
            parser.setBucketName(uri);
            parser.setPrefix("");
          } else if (idx == uri.length() - 1) {
            parser.setBucketName(uri.substring(0, idx));
            parser.setPrefix("");
          } else {
            parser.setBucketName(uri.substring(0, idx));
            String objectKey = uri.substring(idx + 1);
            parser.setObjectKey(objectKey);
            if (isFile) {
              Path p = Paths.get(objectKey);
              Path parent = p.getParent();
              if (null != parent) {
                parser.setPrefix(parent.toString());
              } else {
                parser.setPrefix("");
              }
              parser.setFilename(p.getFileName().toString());
            } else {
              parser.setPrefix(objectKey);
            }
          }
          break;
        case UNKNOWN:
        default:
          LOG.error("url format failed unsupported protocol:[{}]", protocol);
          throw new IllegalArgumentException(
              String.format("url format failed unsupported protocol:[%s]", protocol));
      }
    }
    return parser;
  }

  public ObjectStorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(ObjectStorageType storageType) {
    this.storageType = storageType;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getObjectKey() {
    return objectKey;
  }

  public void setObjectKey(String objectKey) {
    this.objectKey = objectKey;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  @Override
  public String toString() {
    return "{"
        + "storageType="
        + storageType
        + ", bucketName='"
        + bucketName
        + '\''
        + ", prefix='"
        + prefix
        + '\''
        + ", objectKey='"
        + objectKey
        + '\''
        + ", filename='"
        + filename
        + '\''
        + '}';
  }
}
