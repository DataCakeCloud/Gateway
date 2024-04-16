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

package org.apache.kyuubi.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class FileUtils {

  private static final int BUFFER_SIZE = 4 * 1024; // 4K

  public static File createFile(String filePath) throws IOException {
    File f = new File(filePath);
    if (!f.getParentFile().exists()) {
      if (!f.getParentFile().mkdirs()) {
        throw new IOException("Create FilePath Failed: " + f.getParentFile().getAbsolutePath());
      }
    }
    if (!f.exists()) {
      if (!f.createNewFile()) {
        throw new IOException("Create File Failed: " + filePath);
      }
    }
    return f;
  }

  public static boolean deleteAll(File f) {
    if (f == null || !f.exists()) {
      return false;
    }
    if (f.isDirectory()) {
      File[] ff = f.listFiles();
      if (null == ff) {
        return true;
      }
      for (File f1 : ff) {
        deleteAll(f1);
      }
    }
    return f.delete();
  }

  public static boolean deleteAll(String path) {
    File f = new File(path);
    if (!f.exists()) {
      return false;
    }
    return deleteAll(f);
  }

  public static void copy(InputStream from, OutputStream to) throws IOException {
    try (ReadableByteChannel source = Channels.newChannel(from);
        WritableByteChannel destination = Channels.newChannel(to)) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
      while (source.read(buffer) != -1) {
        buffer.flip();
        destination.write(buffer);
        buffer.clear();
      }
    }
  }

  public static File merge(List<Path> paths, String destFilePath) throws IOException {
    if (null == paths) {
      return null;
    }
    Path dest = Paths.get(destFilePath);
    if (!Files.exists(dest)) {
      Files.createDirectories(dest.getParent());
      Files.createFile(dest);
    }

    try (WritableByteChannel writer = Channels.newChannel(Files.newOutputStream(dest))) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
      for (Path path : paths) {
        if (null == path || Files.notExists(path)) {
          continue;
        }
        try (ReadableByteChannel reader = Channels.newChannel(Files.newInputStream(path))) {
          while (reader.read(buffer) != -1) {
            buffer.flip();
            writer.write(buffer);
            buffer.clear();
          }
        }
      }
    }
    return dest.toFile();
  }
}
