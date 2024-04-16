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
// import java.io.BufferedWriter;
// import java.io.File;
// import java.io.IOException;
// import java.nio.charset.StandardCharsets;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.List;
// import java.util.concurrent.BlockingQueue;
// import java.util.concurrent.LinkedBlockingQueue;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.atomic.AtomicBoolean;
// import org.apache.commons.io.FileUtils;
// import org.apache.kyuubi.util.AutoRetry;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// public class StorageOperation {
//
//  private static final Logger LOG = LoggerFactory.getLogger(StorageOperation.class);
//  private static final String UTF8_BOM = "\uFEFF";
//  private static final long WRITE_DATE_TIMEOUT = 3600 * 1000L; // one hour
//
//  private static final int SINGLE_FILE_SIZE_LIMIT = 100 * 1024 * 1024; // 100MB
//  private final BlockingQueue<List<String>> dataQ = new LinkedBlockingQueue<>();
//  private final AtomicBoolean closed = new AtomicBoolean(true);
//  private final AtomicBoolean writtenHeader = new AtomicBoolean(false);
//
//  private final String sessionId;
//  private final String operationId;
//  private final String output;
//  private final String filename;
//  private final String format;
//  private final String delimiter;
//  private final String localParentPath;
//  private final long maxFileSize;
//  private final ObjectStorage objectStorage;
//  private final ObjectStorageUrlParser parser;
//  private BufferedWriter bw;
//  private File currentFile;
//  private int currentFileIndex = 0;
//
//  private long writeBeginTime = 0L;
//
//  public StorageOperation(
//      String sessionId,
//      String operationId,
//      String output,
//      String filename,
//      String format,
//      String delimiter,
//      String localParentPath,
//      long maxFileSize,
//      ObjectStorage os) {
//    this.sessionId = sessionId;
//    this.operationId = operationId;
//    this.output = output;
//    this.filename = filename;
//    this.format = format;
//    this.delimiter = delimiter;
//    this.localParentPath = localParentPath;
//    this.objectStorage = os;
//
//    if (maxFileSize <= 0) {
//      maxFileSize = SINGLE_FILE_SIZE_LIMIT;
//    }
//
//    this.maxFileSize = maxFileSize;
//
//    parser = ObjectStorageUrlParser.parser(output);
//    parser.setPrefix(parser.getPrefix() + sessionId + "/" + operationId + "/");
//  }
//
//  public String getSessionId() {
//    return sessionId;
//  }
//
//  public String getOperationId() {
//    return operationId;
//  }
//
//  public String getOutput() {
//    return output;
//  }
//
//  public String getDelimiter() {
//    return delimiter;
//  }
//
//  private String genFilename(String filename, String format, int index) {
//    if (index == 0) {
//      return filename + "." + format;
//    }
//    return filename + "_" + index + "." + format;
//  }
//
//  public synchronized Task open() throws IOException {
//    Path parent = Paths.get(localParentPath, operationId);
//    if (Files.notExists(parent)) {
//      Files.createDirectories(parent);
//    }
//    Path firstFile = Paths.get(parent.toString(), genFilename(filename, format,
// currentFileIndex));
//    currentFile = firstFile.toFile();
//    if (!currentFile.exists()) {
//      currentFile.createNewFile();
//    }
//    bw = Files.newBufferedWriter(firstFile, StandardCharsets.UTF_8);
//    bw.write(UTF8_BOM);
//    closed.set(false);
//    return new Task();
//  }
//
//  private void check() throws IOException {
//    if (null == bw || isClosed()) {
//      throw new IOException("writer is null or closed");
//    }
//  }
//
//  public boolean isClosed() {
//    return closed.get();
//  }
//
//  public synchronized void close(boolean force) {
//    LOG.debug("Session[{}] Op[{}] close force[{}]", sessionId, operationId, force);
//    if (isClosed()) {
//      return;
//    }
//    closed.set(true);
//    try {
//      if (!force) {
//        while (!dataQ.isEmpty()) {
//          LOG.debug("Session[{}] Op[{}] close but dataQ not empty wait 1s", sessionId,
// operationId);
//          Thread.sleep(1000);
//        }
//      }
//      if (null != bw) {
//        bw.close();
//        bw = null;
//      }
//    } catch (Exception e) {
//      LOG.error("Session[{}] Op[{}] writer close failed and ignore", sessionId, operationId, e);
//    }
//  }
//
//  public synchronized boolean commit() {
//    LOG.info("Session[{}] Op[{}] commit", sessionId, operationId);
//    try {
//      close(false);
//      upload();
//    } catch (Exception e) {
//      LOG.error("Session[{}] Op[{}] upload file failed", sessionId, operationId, e);
//      // delete already upload file
//      objectStorage.deletePath(parser.getBucketName(), parser.getPrefix());
//      return false;
//    } finally {
//      shutdown();
//    }
//    return true;
//  }
//
//  public synchronized void cancel() {
//    close(true);
//  }
//
//  private void upload() throws Exception {
//    Path parent = Paths.get(localParentPath, operationId);
//    File p = parent.toFile();
//    File[] files = p.listFiles();
//    if (!p.exists() || null == files) {
//      LOG.error(
//          "Session[{}] Op[{}] upload file to objectStorage failed reason object files empty",
//          sessionId,
//          operationId);
//      return;
//    }
//    for (File f : files) {
//      LOG.info("Session[{}] Op[{}] upload file[{}]", sessionId, operationId, f.getAbsolutePath());
//      try {
//        AutoRetry.executeWithRetry(
//            () -> {
//              objectStorage.putObject(parser.getBucketName(), parser.getPrefix() + f.getName(),
// f);
//              return true;
//            },
//            3,
//            3000,
//            true);
//      } catch (Exception e) {
//        LOG.error(
//            "Session[{}] Op[{}] upload file to objectStorage failed: [{}]",
//            sessionId,
//            operationId,
//            parser.getPrefix() + f.getName(),
//            e);
//        throw new Exception(e);
//      }
//    }
//  }
//
//  public void clean() {
//    // 清理本地文件
//    Path parent = Paths.get(localParentPath, operationId);
//    LOG.debug("Session[{}] Op[{}] clean path[{}]", sessionId, operationId, parent);
//    try {
//      FileUtils.deleteDirectory(parent.toFile());
//    } catch (IOException e) {
//      LOG.error("Clean path[{}] failed", parent, e);
//    }
//  }
//
//  private synchronized void renewFile() throws IOException {
//    check();
//    bw.close();
//    currentFileIndex += 1;
//    Path path =
//        Paths.get(
//            currentFile.getParentFile().getAbsolutePath(),
//            genFilename(filename, format, currentFileIndex));
//    File newFile = path.toFile();
//    LOG.info("Session[{}] Op[{}] renewFile file[{}]", sessionId, operationId, path);
//    if (!newFile.exists() && !newFile.createNewFile()) {
//      LOG.error("RenewFile failed, file[{}]", newFile.getAbsolutePath());
//      throw new IOException("renewFile failed");
//    }
//    bw = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
//    bw.write(UTF8_BOM);
//    currentFile = newFile;
//  }
//
//  public void writeHeader(String header) throws IOException {
//    if (writtenHeader.get()) {
//      return;
//    }
//    writtenHeader.set(true);
//    bw.write(header);
//    bw.newLine();
//  }
//
//  public void write(List<String> rows) throws IOException {
//    if (null == rows || rows.isEmpty()) {
//      return;
//    }
//    LOG.debug("Session[{}] Op[{}] write rows.size[{}]", sessionId, operationId, rows.size());
//    check();
//    if (writeBeginTime <= 0) {
//      writeBeginTime = System.currentTimeMillis();
//    }
//    try {
//      while (true) {
//        if (dataQ.offer(rows, 3, TimeUnit.SECONDS)) {
//          break;
//        } else {
//          LOG.warn("Session[{}] Op[{}] write dataQ full, sleep 3s", sessionId, operationId);
//          Thread.sleep(3000L);
//        }
//      }
//    } catch (InterruptedException ignore) {
//    }
//  }
//
//  private void writeFile(List<String> rows) throws IOException {
//    if (null == rows || rows.isEmpty()) {
//      LOG.debug("Session[{}] Op[{}] writeFile rows empty", sessionId, operationId);
//      return;
//    }
//    LOG.debug("Session[{}] Op[{}] writeFile rows.size[{}]", sessionId, operationId, rows.size());
//    if (currentFile.length() > maxFileSize) {
//      renewFile();
//    }
//    try {
//      for (String row : rows) {
//        bw.write(row);
//        bw.newLine();
//      }
//    } finally {
//      bw.flush();
//    }
//  }
//
//  private boolean writeTimeout() {
//    return writeBeginTime > 0 && System.currentTimeMillis() - writeBeginTime > WRITE_DATE_TIMEOUT;
//  }
//
//  public void shutdown() {
//    objectStorage.close();
//  }
//
//  class Task implements Runnable {
//
//    @Override
//    public void run() {
//      List<String> rows;
//      try {
//        while (!isClosed() || !dataQ.isEmpty()) {
//          rows = dataQ.poll(3, TimeUnit.SECONDS);
//          if (null != rows) {
//            writeFile(rows);
//          } else {
//            LOG.warn("Session[{}] Op[{}] write dataQ empty, sleep 3s", sessionId, operationId);
//            Thread.sleep(3000L);
//          }
//          if (writeTimeout()) {
//            LOG.warn("Session[{}] Op[{}] write timeout", sessionId, operationId);
//            break;
//          }
//        }
//      } catch (InterruptedException ignore) {
//      } catch (IOException e) {
//        LOG.error(
//            "Session[{}] Op[{}] writeFile failed so cancel handle", sessionId, operationId, e);
//        cancel();
//      }
//    }
//  }
// }
