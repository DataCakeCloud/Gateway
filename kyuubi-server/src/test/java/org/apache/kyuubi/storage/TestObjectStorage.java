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
import com.amazonaws.regions.Regions;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.kyuubi.storage.impl.*;

public class TestObjectStorage {

  static void testS3() throws IOException {
    System.setProperty("kyuubi.testing", "true");
    String bucket = "shareit.deploy.us-east-1";
    String prefix = "BDP/BDP-kyuubi/storage";
    String suffix = ".md";
    String testUploadFile = "./README.md";
    String downloadPath = "./test.md";
    S3ObjectStorage s3 = new S3ObjectStorage(Regions.US_EAST_1.getName());

    test(s3, bucket, prefix, suffix, testUploadFile, downloadPath);
  }

  static void testObs() throws IOException {
    System.setProperty("kyuubi.testing", "true");
    String endpoint = "obs.ap-southeast-3.myhuaweicloud.com";
    String bucket = "bdp-deploy-sg";
    String prefix = "BDP/temp";
    String suffix = ".md";
    String testUploadFile = "./README.md";
    String downloadPath = "./test.md";
    ObsObjectStorage obs = new ObsObjectStorage(endpoint, null, null);

    test(obs, bucket, prefix, suffix, testUploadFile, downloadPath);
  }

  static void testKs3() throws IOException {
    String bucket = "ninebot";
    String prefix = "jars";
    String suffix = ".jar";
    String testUploadFile = "./README.md";
    String downloadPath = "./test.md";
    Ks3ObjectStorage ks3 = new Ks3ObjectStorage("", "", "");

    test(ks3, bucket, prefix, suffix, testUploadFile, downloadPath);
  }

  static void testGs() throws IOException {
    // gs://bdp-common-sg3-prod/
    String bucket = "bdp-common-sg3-prod";
    String prefix = "tmp";
    String suffix = ".md";
    String testUploadFile = "./README.md";
    String downloadPath = "./test.md";
    GsObjectStorage gs = new GsObjectStorage();

    test(gs, bucket, prefix, suffix, testUploadFile, downloadPath);
  }

  static void testOss() throws IOException {
    String ak = "";
    String sk = "";
    String endpoint = "oss-ap-southeast-1.aliyuncs.com";
    String bucket = "datacake";
    String prefix = "tmp";
    String suffix = ".md";
    String testUploadFile = "./README.md";
    String downloadPath = "./test.md";
    OssObjectStorage oss = null;
    try {
      oss = new OssObjectStorage(endpoint, ak, sk);
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }

    test(oss, bucket, prefix, suffix, testUploadFile, downloadPath);
  }

  static void test(
      ObjectStorage os,
      String bucket,
      String prefix,
      String suffix,
      String uploadFilePath,
      String downloadFilePath)
      throws IOException {
    String storageName = os.getClass().getSimpleName();

    File testUploadFile = new File(uploadFilePath);
    String object = prefix + "/" + testUploadFile.getName();
    System.out.println(storageName + " test object: " + object);

    String result = os.putObject(bucket, object, testUploadFile);
    System.out.println(storageName + " pubObject result: " + result);

    result = os.putObject(bucket, object, testUploadFile, "text/plain");
    System.out.println(storageName + " pubObject2 result: " + result);

    File f = os.getObject(bucket, object, downloadFilePath);
    System.out.println(storageName + " getObject result: " + f.getAbsoluteFile());

    List<String> objects = os.listObjects(bucket, prefix);
    System.out.println(storageName + " listObjects result: " + objects);

    objects = os.listObjects(bucket, prefix, suffix);
    System.out.println(storageName + " listObjects2 result: " + objects);

    String url = os.signObject(bucket, object, Duration.ofHours(3));
    System.out.println(storageName + " signObject result: " + url);

    os.putObject(bucket, object + 2, testUploadFile);
    List<String> signUrls = os.signObjects(bucket, prefix, suffix, Duration.ofHours(3));
    System.out.println(storageName + " signObjects result: " + signUrls);

    long size = os.objectSize(bucket, object);
    System.out.println(storageName + " objectSize result: " + size);

    size = os.objectsSize(bucket, prefix, suffix);
    System.out.println(storageName + " objectSize2 result: " + size);

    result = os.deleteObject(bucket, object);
    System.out.println(storageName + " deleteObject result: " + result);

    os.close();
  }

  public static void main(String[] args) throws IOException {
    testS3();
    testObs();
    testKs3();
    testGs();
    testOss();
  }
}
