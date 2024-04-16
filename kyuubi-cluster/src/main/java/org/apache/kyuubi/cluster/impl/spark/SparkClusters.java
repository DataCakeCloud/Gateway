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

package org.apache.kyuubi.cluster.impl.spark;

import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.Cluster;
import org.apache.kyuubi.cluster.Commands;
import org.apache.kyuubi.cluster.selector.Selectors;
import org.apache.kyuubi.k8s.KubernetesClusters;
import org.apache.kyuubi.util.YamlLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkClusters extends KubernetesClusters {

  private static final Logger LOG = LoggerFactory.getLogger(SparkClusters.class);

  private static final String SPARK_CONFIG_PATH = "spark";

  public static SparkClusters load(String clusterConfigPath) throws Exception {
    if (StringUtils.isBlank(clusterConfigPath)) {
      throw new InvalidParameterException(
          "Load spark clusters failed reason clusterConfigPath empty.");
    }
    String clusterConfigFile =
        Paths.get(clusterConfigPath, SPARK_CONFIG_PATH, "cluster.yaml").toString();
    SparkClusters c = YamlLoader.load(clusterConfigFile, SparkClusters.class);
    if (null == c || null == c.clusters || c.clusters.isEmpty()) {
      throw new InvalidParameterException(
          "Load spark clusters failed, clusterConfigFile is [" + clusterConfigFile + "].");
    }

    for (Cluster cluster : c.clusters) {
      cluster.setConfigPath(clusterConfigPath);
    }

    String commandConfigFile =
        Paths.get(clusterConfigPath, SPARK_CONFIG_PATH, "command.yaml").toString();
    Commands commands = Commands.load(commandConfigFile);
    c.setCommands(commands);

    String selectorConfigFile =
        Paths.get(clusterConfigPath, SPARK_CONFIG_PATH, "selector.yaml").toString();
    Selectors selectors = Selectors.load(selectorConfigFile);
    c.setSelectors(selectors);

    return c;
  }

  @Override
  public String getClusterType() {
    return "spark";
  }

  @Override
  protected List<Cluster> matchClusters(List<String> clusterTags, List<String> commandTags) {
    List<Cluster> clusters = matchTags(clusterTags, commandTags);
    if (clusters.isEmpty() || clusters.size() == 1) {
      return clusters;
    }

    computeUsage(clusters);
    LOG.info(
        "Select cluster matched cluster usages {}.",
        clusters.stream().map(Cluster::getUsage).collect(Collectors.toList()));

    List<Cluster> nonOverload = filterOverloadCluster(clusters);
    if (nonOverload.isEmpty()) {
      return clusters;
    } else {
      return nonOverload;
    }
  }

  @Override
  public String toString() {
    return "{"
        + "clusters="
        + clusters
        + ", commands="
        + commands
        + ", selectors="
        + selectors
        + '}';
  }
}
