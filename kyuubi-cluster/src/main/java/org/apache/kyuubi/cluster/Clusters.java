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

import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.selector.ClusterSelector;
import org.apache.kyuubi.cluster.selector.ClusterSelectorType;
import org.apache.kyuubi.cluster.selector.Selectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Clusters {

  private static final Logger LOG = LoggerFactory.getLogger(Clusters.class);

  protected List<String> supports;
  protected List<Cluster> clusters;
  protected Commands commands;
  protected Selectors selectors;

  public List<String> getSupports() {
    return supports;
  }

  public void setSupports(List<String> supports) {
    this.supports = supports;
  }

  public List<Cluster> getClusters() {
    return clusters;
  }

  public void setClusters(List<Cluster> clusters) {
    this.clusters = clusters;
  }

  public Commands getCommands() {
    return commands;
  }

  public void setCommands(Commands commands) {
    this.commands = commands;
  }

  public Selectors getSelectors() {
    return selectors;
  }

  public void setSelectors(Selectors selectors) {
    this.selectors = selectors;
  }

  public abstract String getClusterType();

  public Cluster findCluster(String clusterId) {
    if (StringUtils.isEmpty(clusterId) || null == clusters) {
      return null;
    }
    for (Cluster clu : clusters) {
      if (clu.getId().equals(clusterId)) {
        return clu.clone();
      }
    }
    return null;
  }

  public Commands.Command findCommand(String name) {
    if (StringUtils.isEmpty(name) || null == commands || null == commands.getCommands()) {
      return null;
    }
    for (Commands.Command cmd : commands.getCommands()) {
      if (cmd.getName().equals(name)) {
        return cmd;
      }
    }
    return null;
  }

  protected List<Cluster> matchClusterTags(List<String> clusterTags) {
    List<Cluster> matcher = new ArrayList<>();
    // 匹配标签，集群标签全部包含匹配标签即可
    for (Cluster cluster : clusters) {
      List<String> allTags = cluster.getTags();
      boolean match = true;
      for (String tag : clusterTags) {
        if (!allTags.contains(tag)) {
          match = false;
          break;
        }
      }
      if (match) {
        Cluster clu = cluster.clone();
        if (null != clu) {
          matcher.add(clu);
        }
      }
    }

    return matcher;
  }

  protected List<Commands.Command> matchCommandTags(List<String> commandTags) {
    // 匹配 command 标签
    List<Commands.Command> matcher = new ArrayList<>();
    // 匹配标签，匹配的标签必须全部包含命令标签
    for (Commands.Command cmd : commands.getCommands()) {
      List<String> allTags = cmd.getTags();
      boolean match = true;
      for (String tag : commandTags) {
        if (!allTags.contains(tag)) {
          match = false;
          break;
        }
      }
      if (match) {
        matcher.add(cmd);
      }
    }

    return matcher;
  }

  protected List<Cluster> matchClusterAndCommand(
      List<Cluster> clusters, List<Commands.Command> commands) {
    Map<String, Commands.Command> map = new HashMap<>();
    for (Commands.Command cmd : commands) {
      map.put(cmd.getId(), cmd);
    }
    for (Iterator<Cluster> it = clusters.iterator(); it.hasNext(); ) {
      Cluster cluster = it.next();
      List<String> activeCommands = cluster.getActiveCommands();
      for (String ac : activeCommands) {
        if (map.containsKey(ac)) {
          cluster.setCommand(map.get(ac));
          break;
        }
      }
      if (null == cluster.getCommand()) {
        it.remove();
      }
    }
    return clusters;
  }

  protected List<Cluster> matchTags(List<String> clusterTags, List<String> commandTags) {
    List<Cluster> clusters = matchClusterTags(clusterTags);
    List<Commands.Command> commands = matchCommandTags(commandTags);
    return matchClusterAndCommand(clusters, commands);
  }

  protected Cluster authCluster(Cluster cluster) throws IllegalAccessException {
    List<String> na = cluster.getNeedAccount();
    if (null == na) {
      return cluster;
    }
    /*
    auth cluster
     */
    return cluster;
  }

  protected Cluster selector(List<Cluster> clusters, List<String> selectorNames) {
    // 如果匹配到多个未超载的集群，按照匹配算法进行顺序筛选
    for (String name : selectorNames) {
      ClusterSelector cs = selectors.getSelector(name);
      if (null == cs) {
        continue;
      }
      List<Cluster> c = cs.select(clusters);
      if (c.size() == 1) {
        return c.get(0);
      }
    }
    return null;
  }

  public Cluster selectCluster(
      String taskId, List<String> clusterTags, List<String> commandTags, List<String> selectorNames)
      throws Exception {
    if (clusterTags.isEmpty()) {
      LOG.warn("Batch[{}] selectCluster clusterTags [{}] empty.", taskId, clusterTags);
      return null;
    }
    String randomSelectName = ClusterSelectorType.RANDOM.name().toLowerCase();
    if (null == selectorNames) {
      selectorNames = Collections.singletonList(randomSelectName);
    } else if (!selectorNames.contains(randomSelectName)) {
      selectorNames = new ArrayList<>(selectorNames);
      selectorNames.add(randomSelectName);
    }

    LOG.info(
        "Batch[{}] selectCluster clusterTags {}, commandTags {}, selectorNames {}.",
        taskId,
        clusterTags,
        commandTags,
        selectorNames);

    List<Cluster> clusters = matchClusters(clusterTags, commandTags);

    LOG.debug("Batch[{}] selectCluster match: {}", taskId, clusters);
    if (clusters.isEmpty()) {
      LOG.error("Batch[{}] selectCluster not match any clusters.", taskId);
      return null;
    } else if (clusters.size() == 1) {
      LOG.info("Batch[{}] selectCluster match clusters only one.", taskId);
      return authCluster(clusters.get(0));
    }

    return authCluster(selector(clusters, selectorNames));
  }

  public Cluster getCluster(String taskId, String clusterId, String commandName) {
    LOG.info("Batch[{}] getCluster cluster: [{}], command: [{}].", taskId, clusterId, commandName);
    Cluster clu = findCluster(clusterId);
    if (null == clu) {
      return null;
    }
    Commands.Command cmd = findCommand(commandName);
    if (null == cmd) {
      cmd = new Commands.Command();
    }
    clu.setCommand(cmd);
    return clu;
  }

  protected abstract List<Cluster> matchClusters(
      List<String> clusterTags, List<String> commandTags);
}
