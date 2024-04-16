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

package org.apache.kyuubi.authentication;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.util.AutoRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationManager {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationManager.class);

  public static AuthenticationManager load(String clusterConfigPath) throws Exception {
    return new AuthenticationManager(clusterConfigPath);
  }

  AuthenticationManager(String clusterConfigPath) {}

  public Group buildAuthGroup(
      String groupId,
      String groupName,
      String keytabPrefix,
      String principalHost,
      String principalRealm) {
    Group grp = new Group();
    grp.setId(groupId);
    grp.setName(groupName);
    grp.setAuthType("KERBEROS");
    grp.setKeytab(Paths.get(keytabPrefix, groupName, ".keytab", groupName + ".keytab").toString());
    grp.setPrincipal(groupName + principalHost + principalRealm);
    return grp;
  }

  public Group auth(Group group, String outputResPath) throws Exception {
    if (null == group) {
      throw new IllegalArgumentException("auth group failed, group is null");
    }
    switch (group.getAuthType().toUpperCase(Locale.ROOT)) {
      case "KERBEROS":
        kinit(group, outputResPath);

        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "unsupported authType group[%s], authType[%s]", group, group.getAuthType()));
    }

    return group;
  }

  public Group auth(
      String groupId,
      String groupName,
      String keytabPrefix,
      String principalHost,
      String principalRealm,
      String outputResPath)
      throws Exception {
    Group group = buildAuthGroup(groupId, groupName, keytabPrefix, principalHost, principalRealm);

    auth(group, outputResPath);

    return group;
  }

  public void kinit(Group group, String cachePath) throws Exception {
    String keytab = group.getKeytab();
    String principal = group.getPrincipal();
    try {
      kinit(keytab, principal, cachePath);
    } catch (Exception e) {
      throw new Exception(
          "Failed to kinit with group: " + group.getName() + "\n" + e.getMessage(), e);
    }
  }

  public void kinit(String keytab, String principal, String cachePath) throws Exception {
    if (Files.notExists(Paths.get(keytab))) {
      throw new IllegalArgumentException(
          String.format("Failed to kinit file keytab:[%s] not found", keytab));
    }
    List<String> commands;
    if (StringUtils.isEmpty(cachePath)) {
      commands = Arrays.asList("kinit", "-kt", keytab, principal);
    } else {
      commands = Arrays.asList("kinit", "-c", cachePath, "-kt", keytab, principal);
    }
    LOG.info("kinit commands: {}", commands);
    ProcessBuilder proc = new ProcessBuilder(commands).inheritIO();
    AutoRetry.executeWithRetry(
        () -> {
          Process process = proc.start();
          int code = process.waitFor();
          if (code == 0) {
            LOG.info("Successfully {}", commands);
          } else {
            throw new Exception(String.format("Failed to kinit with code:[%d]", code));
          }
          return true;
        },
        3,
        3000,
        true);
  }
}
