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

package org.apache.kyuubi.cluster.impl.hive;

import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.Cluster;
import org.apache.kyuubi.cluster.Clusters;
import org.apache.kyuubi.cluster.selector.Selectors;
import org.apache.kyuubi.util.YamlLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveClusters extends Clusters {

  private static final Logger LOG = LoggerFactory.getLogger(HiveClusters.class);
  private static final String HIVE_CONFIG_PATH = "hive";

  public static HiveClusters load(String clusterConfigPath) throws Exception {
    if (StringUtils.isBlank(clusterConfigPath)) {
      throw new IllegalArgumentException(
          "Load hive clusters failed reason clusterConfigPath empty.");
    }
    String clusterConfigFile =
        Paths.get(clusterConfigPath, HIVE_CONFIG_PATH, "cluster.yaml").toString();
    HiveClusters c = YamlLoader.load(clusterConfigFile, HiveClusters.class);
    if (null == c || null == c.clusters || c.clusters.isEmpty()) {
      throw new IllegalArgumentException(
          "Load hive clusters failed, clusterConfigFile is [" + clusterConfigFile + "].");
    }

    for (Cluster cluster : c.clusters) {
      cluster.setConfigPath(clusterConfigPath);
    }

    String selectorConfigFile =
        Paths.get(clusterConfigPath, HIVE_CONFIG_PATH, "selector.yaml").toString();
    Selectors selectors = Selectors.load(selectorConfigFile);
    c.setSelectors(selectors);
    return c;
  }

  @Override
  public String getClusterType() {
    return "hive";
  }

  @Override
  protected List<Cluster> matchClusters(List<String> clusterTags, List<String> commandTags) {
    return matchClusterTags(clusterTags);
  }
}
