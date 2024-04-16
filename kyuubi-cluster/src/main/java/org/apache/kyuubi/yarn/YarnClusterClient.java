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

package org.apache.kyuubi.yarn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kyuubi.authentication.Group;
import org.apache.kyuubi.cluster.Cluster;
import org.apache.kyuubi.util.AutoRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnClusterClient {

  private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClient.class);

  private final Cluster cluster;
  private final YarnClient client;
  private Group group;
  private final boolean authentication;

  YarnClusterClient(Cluster cluster) throws Exception {
    this.cluster = cluster;
    this.group = null;
    this.authentication =
        "true".equalsIgnoreCase(System.getProperty("kyuubi.authentication.enabled"));
    String configPath = cluster.getYarnClusterConfigPath();
    if (Files.notExists(Paths.get(configPath))) {
      LOG.error("cluster config path not exists [{}]", configPath);
      throw new IllegalStateException("cluster config path not exists");
    }

    Configuration conf = new Configuration();
    conf.addResource(new Path(configPath, "core-site.xml"));
    conf.addResource(new Path(configPath, "yarn-site.xml"));
    conf.addResource(new Path(configPath, "hdfs-site.xml"));

    if (authentication) {
      String principal = System.getProperty("kyuubi.kinit.principal");
      String keytab = System.getProperty("kyuubi.kinit.keytab");
      group = new Group();
      group.setPrincipal(principal);
      group.setKeytab(keytab);
      UserGroupInformation.setConfiguration(conf);
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
      } catch (IOException e) {
        LOG.error("init yarn client failed", e);
        throw e;
      }
    }
    client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
  }

  YarnClusterClient(Cluster cluster, Group group) throws Exception {
    this.cluster = cluster;
    this.group = group;
    this.authentication =
        "true".equalsIgnoreCase(System.getProperty("kyuubi.authentication.enabled"));
    String configPath = cluster.getYarnClusterConfigPath();
    if (Files.notExists(Paths.get(configPath))) {
      LOG.error("cluster config path not exists [{}]", configPath);
      throw new IllegalStateException("cluster config path not exists");
    }

    Configuration conf = new Configuration();
    conf.addResource(new Path(configPath, "core-site.xml"));
    conf.addResource(new Path(configPath, "yarn-site.xml"));
    conf.addResource(new Path(configPath, "hdfs-site.xml"));

    if ("true".equalsIgnoreCase(System.getProperty("kyuubi.authentication.enabled"))) {
      String principal = group.getPrincipal();
      String keytab = group.getKeytab();
      UserGroupInformation.setConfiguration(conf);
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
      } catch (IOException e) {
        LOG.error("init yarn client failed", e);
        throw e;
      }
    }
    client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
  }

  interface Function<T, R> {
    R call(T t) throws IOException, YarnException;
  }

  private <T, R> R authentication(Function<T, R> func, T t)
      throws IOException, InterruptedException {
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(
            group.getPrincipal(), group.getKeytab());
    LOG.info(
        "YarnClient authentication[{}] curUser: [{}]",
        t,
        UserGroupInformation.getCurrentUser().getUserName());
    return ugi.doAs((PrivilegedExceptionAction<R>) () -> func.call(t));
  }

  public List<ApplicationReport> findApplicationByTag(String tag) throws Exception {
    LOG.info("YarnClient findApp tag: [{}]", tag);
    if (authentication) {
      return authentication(this::findApplicationByTag0, tag);
    } else {
      return findApplicationByTag0(tag);
    }
  }

  public void deleteApplicationByTag(String tag) throws Exception {
    LOG.info("DeleteApplicationByTag cluster: [{}], tag: [{}]", cluster.getId(), tag);
    if (authentication) {
      authentication(this::deleteApplicationByTag0, tag);
    } else {
      deleteApplicationByTag0(tag);
    }
  }

  private List<ApplicationReport> findApplicationByTag0(String tag)
      throws IOException, YarnException {
    return client.getApplications(null, null, Collections.singleton(tag));
  }

  private Void deleteApplicationByTag0(String tag) throws IOException, YarnException {
    LOG.info(
        "YarnClient deleteApp tag: [{}] curUser: [{}]",
        tag,
        UserGroupInformation.getCurrentUser().getUserName());
    List<ApplicationReport> apps = findApplicationByTag0(tag);
    if (apps.isEmpty()) {
      LOG.error(
          "DeleteApplicationByTag app not found cluster: [{}], tag: [{}]", cluster.getId(), tag);
      throw new YarnException(String.format("App not found by this tag: [%s]", tag));
    }
    LOG.debug("DeleteApplicationByTag pods size: [{}]", apps.size());
    try {
      for (ApplicationReport app : apps) {
        AutoRetry.executeWithRetry(
            () -> {
              client.killApplication(app.getApplicationId());
              LOG.debug("DeleteAppByTag tag: [{}], appId: [{}]", tag, app.getApplicationId());
              return true;
            },
            3,
            3000,
            true);
      }
    } catch (Exception e) {
      LOG.error(
          "DeleteApplicationByTag pod failed cluster: [{}], tag: [{}]", cluster.getId(), tag, e);
    }
    return null;
  }

  public void close() {
    if (null != client) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("close yarn client error", e);
      }
    }
  }
}
