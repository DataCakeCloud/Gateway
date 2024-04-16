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

package org.apache.kyuubi.cluster.impl.trino;

import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.Cluster;
import org.apache.kyuubi.cluster.Clusters;
import org.apache.kyuubi.cluster.impl.spark.SparkClusters;
import org.apache.kyuubi.cluster.selector.Selectors;
import org.apache.kyuubi.util.YamlLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrinoClusters extends Clusters {

  private static final Logger LOG = LoggerFactory.getLogger(SparkClusters.class);
  private static final String TRINO_CONFIG_PATH = "trino";

  public static TrinoClusters load(String clusterConfigPath) throws Exception {
    if (StringUtils.isBlank(clusterConfigPath)) {
      throw new IllegalArgumentException(
          "Load trino clusters failed reason clusterConfigPath empty.");
    }
    String clusterConfigFile =
        Paths.get(clusterConfigPath, TRINO_CONFIG_PATH, "cluster.yaml").toString();
    TrinoClusters c = YamlLoader.load(clusterConfigFile, TrinoClusters.class);
    if (null == c || null == c.clusters || c.clusters.isEmpty()) {
      throw new IllegalArgumentException(
          "Load trino clusters failed, clusterConfigFile is [" + clusterConfigFile + "].");
    }

    for (Cluster cluster : c.clusters) {
      cluster.setConfigPath(clusterConfigPath);
    }

    String selectorConfigFile =
        Paths.get(clusterConfigPath, TRINO_CONFIG_PATH, "selector.yaml").toString();
    Selectors selectors = Selectors.load(selectorConfigFile);
    c.setSelectors(selectors);
    return c;
  }

  @Override
  public String getClusterType() {
    return "trino";
  }

  @Override
  protected List<Cluster> matchClusters(List<String> clusterTags, List<String> commandTags) {
    return matchClusterTags(clusterTags);
  }
}
