/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.kyuubi.storage;
//
// import java.io.IOException;
// import java.nio.file.Paths;
// import java.util.Map;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.ThreadPoolExecutor;
// import org.apache.commons.io.FileUtils;
// import org.apache.commons.lang3.StringUtils;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// public class StorageSession {
//
//  private static final Logger LOG = LoggerFactory.getLogger(StorageSession.class);
//
//  private static final String WORK_PATH = System.getenv("KYUUBI_WORK_DIR_ROOT");
//  private static final String STORAGE_PATH =
//      StringUtils.isNotEmpty(WORK_PATH)
//          ? Paths.get(WORK_PATH, "storage").toString()
//          : Paths.get("work", "storage").toString();
//
//  private final String sessionId;
//  private final String output;
//  private final String filename;
//  private final String format;
//  private final String delimiter;
//  private final long fileMaxSize;
//  private final ObjectStorage os;
//  private final ThreadPoolExecutor tp;
//  private final Map<String, StorageOperation> map = new ConcurrentHashMap<>();
//
//  public StorageSession(
//      String sessionId,
//      String output,
//      String filename,
//      String format,
//      String delimiter,
//      long fileMaxSize,
//      ObjectStorage os,
//      ThreadPoolExecutor tp) {
//    this.sessionId = sessionId;
//    this.output = output;
//    this.filename = filename;
//    this.format = format;
//    this.delimiter = delimiter;
//    this.fileMaxSize = fileMaxSize;
//    this.os = os;
//    this.tp = tp;
//  }
//
//  public StorageOperation openStorageOperation(String operationId) throws Exception {
//    if (map.containsKey(operationId)) {
//      return map.get(operationId);
//    } else {
//      StorageOperation handle =
//          new StorageOperation(
//              sessionId,
//              operationId,
//              output,
//              filename,
//              format,
//              delimiter,
//              Paths.get(STORAGE_PATH, sessionId).toString(),
//              fileMaxSize,
//              os);
//      StorageOperation.Task task = handle.open();
//      map.put(operationId, handle);
//      tp.execute(task);
//      LOG.info(
//          "OpenStorageOperation current storageExecPoolSize[{}], storageExecPoolActiveCount[{}]",
//          tp.getPoolSize(),
//          tp.getActiveCount());
//      return handle;
//    }
//  }
//
//  public String getSessionId() {
//    return sessionId;
//  }
//
//  public StorageOperation getOperation(String operationId) {
//    return map.get(operationId);
//  }
//
//  public void removeOperation(String operationId) {
//    StorageOperation op = map.remove(operationId);
//    if (null != op) {
//      op.cancel();
//      op.clean();
//      op.shutdown();
//    }
//  }
//
//  public void commit() {
//    for (StorageOperation op : map.values()) {
//      op.commit();
//    }
//    clean();
//  }
//
//  public void cancel() {
//    for (StorageOperation op : map.values()) {
//      op.cancel();
//      op.shutdown();
//    }
//    clean();
//  }
//
//  public void clean() {
//    for (StorageOperation op : map.values()) {
//      op.clean();
//    }
//    map.clear();
//    try {
//      FileUtils.deleteDirectory(Paths.get(STORAGE_PATH, sessionId).toFile());
//    } catch (IOException e) {
//      LOG.error("Session[{}] clean failed and ignore", sessionId, e);
//    }
//  }
// }
