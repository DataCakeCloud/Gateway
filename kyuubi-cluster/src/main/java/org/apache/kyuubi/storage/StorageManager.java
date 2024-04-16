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
// import java.util.Map;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.LinkedBlockingQueue;
// import java.util.concurrent.ThreadPoolExecutor;
// import java.util.concurrent.TimeUnit;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// public class StorageManager {
//
//  private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);
//
//  private static final int DEFAULT_CORE_POOL_SIZE = 100;
//
//  private static final long DEFAULT_KEEP_ALIVE_TIME_SECONDS = 60;
//
//  private final Map<String, StorageSession> map = new ConcurrentHashMap<>();
//
//  private final ThreadPoolExecutor tp;
//
//  public StorageManager() {
//    this(0, 0, 0, 0L);
//  }
//
//  public StorageManager(
//      int corePoolSize, int maxPoolSize, int waitQueueSize, long keepAliveTimeMillis) {
//    if (corePoolSize <= 0) {
//      corePoolSize = DEFAULT_CORE_POOL_SIZE;
//    }
//    if (maxPoolSize <= 0) {
//      maxPoolSize = corePoolSize * 2;
//    }
//    if (waitQueueSize <= 0) {
//      waitQueueSize = maxPoolSize;
//    }
//    if (keepAliveTimeMillis <= 0) {
//      keepAliveTimeMillis = DEFAULT_KEEP_ALIVE_TIME_SECONDS * 1000L;
//    }
//    tp =
//        new ThreadPoolExecutor(
//            corePoolSize,
//            maxPoolSize,
//            keepAliveTimeMillis,
//            TimeUnit.MILLISECONDS,
//            new LinkedBlockingQueue<>(waitQueueSize),
//            r -> {
//              Thread t = new Thread(r);
//              t.setName("StorageSession: Thread-" + t.getId());
//              return t;
//            });
//    tp.allowCoreThreadTimeOut(true);
//  }
//
//  public synchronized StorageSession newStorageSession(
//      String sessionId,
//      String output,
//      String filename,
//      String format,
//      String delimiter,
//      long fileMaxSize,
//      ObjectStorage os) {
//    return map.computeIfAbsent(
//        sessionId,
//        a ->
//            new StorageSession(
//                sessionId, output, filename, format, delimiter, fileMaxSize, os, tp));
//  }
//
//  public StorageOperation findOperation(String operationId) {
//    for (StorageSession session : map.values()) {
//      StorageOperation op = session.getOperation(operationId);
//      if (null != op) {
//        return op;
//      }
//    }
//    return null;
//  }
//
//  public StorageSession findSessionByOperation(String operationId) {
//    for (StorageSession session : map.values()) {
//      StorageOperation op = session.getOperation(operationId);
//      if (null != op) {
//        return session;
//      }
//    }
//    return null;
//  }
//
//  public StorageSession getSession(String sessionId) {
//    return map.get(sessionId);
//  }
//
//  public void cancelSession(String sessionId) {
//    StorageSession session = getSession(sessionId);
//    if (null != session) {
//      session.cancel();
//    }
//  }
//
//  public void closeSession(String sessionId) {
//    LOG.info("CloseSession session[{}]", sessionId);
//    StorageSession session = getSession(sessionId);
//    if (null != session) {
//      tp.execute(
//          () -> {
//            LOG.info("Session[{}] commit", sessionId);
//            if (null != map.remove(sessionId)) {
//              session.commit();
//            }
//          });
//    }
//  }
//
//  public int getExecPoolSize() {
//    return tp.getPoolSize();
//  }
//
//  public int getActiveCount() {
//    return tp.getActiveCount();
//  }
//
//  public int getWaitQueueSize() {
//    if (null != tp.getQueue()) {
//      return tp.getQueue().size();
//    }
//    return 0;
//  }
//
//  public void shutdown(long duration) {
//    for (StorageSession session : map.values()) {
//      session.commit();
//    }
//    tp.shutdown();
//    try {
//      tp.awaitTermination(duration, TimeUnit.MILLISECONDS);
//    } catch (InterruptedException e) {
//      LOG.warn(
//          String.format(
//              "Exceeded timeout(%d ms) to wait the exec-pool shutdown gracefully", duration),
//          e);
//    }
//  }
// }
