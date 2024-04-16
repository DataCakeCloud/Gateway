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

package org.apache.kyuubi.cluster;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.impl.flink.FlinkClusters;
import org.apache.kyuubi.cluster.impl.hive.HiveClusters;
import org.apache.kyuubi.cluster.impl.spark.SparkClusters;
import org.apache.kyuubi.cluster.impl.trino.TrinoClusters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);

  private final Map<TaskType, Clusters> clusterMap = new HashMap<>(4);

  public static ClusterManager load(String clusterConfigPath) throws Exception {
    return new ClusterManager(clusterConfigPath);
  }

  private ClusterManager(String clusterConfigPath) throws Exception {
    if (StringUtils.isBlank(clusterConfigPath)) {
      LOG.error("Load clusters failed reason clusterConfigPath empty.");
      throw new InvalidParameterException("Load clusters failed reason clusterConfigPath empty.");
    }
    SparkClusters sparkClusters = SparkClusters.load(clusterConfigPath);
    for (String support : sparkClusters.getSupports()) {
      clusterMap.put(TaskType.fromName(support), sparkClusters);
    }

    TrinoClusters trinoClusters = TrinoClusters.load(clusterConfigPath);
    for (String support : trinoClusters.getSupports()) {
      clusterMap.put(TaskType.fromName(support), trinoClusters);
    }

    HiveClusters hiveClusters = HiveClusters.load(clusterConfigPath);
    for (String support : hiveClusters.getSupports()) {
      clusterMap.put(TaskType.fromName(support), hiveClusters);
    }

    FlinkClusters flinkClusters = FlinkClusters.load(clusterConfigPath);
    for (String support : flinkClusters.getSupports()) {
      clusterMap.put(TaskType.fromName(support), flinkClusters);
    }
  }

  public Cluster selectCluster(
      String type,
      String taskId,
      List<String> clusterTags,
      List<String> commandTags,
      List<String> selectorNames,
      boolean dynamic)
      throws Exception {
    return selectCluster(type, taskId, clusterTags, commandTags, selectorNames, null, dynamic);
  }

  public Cluster selectCluster(
      String type,
      String taskId,
      List<String> clusterTags,
      List<String> commandTags,
      List<String> selectorNames,
      Map<String, String> params,
      boolean dynamic)
      throws Exception {
    LOG.info(
        "SelectCluster batch[{}] taskType: {}, dynamic: {}, params: {}",
        taskId,
        type,
        dynamic,
        params);
    TaskType taskType = TaskType.fromName(type);
    Clusters clusters = clusterMap.get(taskType);
    if (null == clusters) {
      return null;
    }
    return clusters.selectCluster(taskId, clusterTags, commandTags, selectorNames);
  }

  public Cluster getCluster(String type, String taskId, String clusterId, String commandName) {
    TaskType taskType = TaskType.fromName(type);
    Clusters clusters = clusterMap.get(taskType);
    if (null != clusters) {
      return clusters.getCluster(taskId, clusterId, commandName);
    }
    return null;
  }

  public Clusters getClusters(String type) {
    return clusterMap.get(TaskType.fromName(type));
  }
}
