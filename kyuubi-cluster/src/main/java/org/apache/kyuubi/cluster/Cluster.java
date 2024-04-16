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

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster implements Comparable<Cluster>, Cloneable {

  private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

  public static final String KUBERNETES_CONFIG_PATH = "kubernetes";
  public static final String YARN_CONFIG_PATH = "yarn";
  public static final String SPARK_CONFIG_PATH = "spark";
  public static final String TEMP_PATH = "/tmp";
  public static final String K8S_PREFIX = "k8s://";
  public static final String YARN_PREFIX = "yarn";

  private String id;
  private String name;
  private String provider;
  private String region;
  private String master;
  private String webUI;
  private String catalog;
  private String namespace;
  private String serverName;
  private int priority = 10;
  private List<String> conf;

  @SerializedName("max_usage")
  private double maxUsage = 1; // 最大允许资源负载

  private boolean scalable;

  @SerializedName("need_account")
  private List<String> needAccount;

  @SerializedName("need_compute_usage")
  private boolean needComputeUsage;

  private List<Instance> instances;
  private List<String> tags;

  @SerializedName("active_commands")
  private List<String> activeCommands;

  private Commands.Command command;
  private double usage = 2;
  private String configPath;

  public Cluster() {}

  public Cluster(String id) {
    this.id = id;
  }

  public Cluster(String id, String configPath) {
    this.id = id;
    this.configPath = configPath;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getProvider() {
    return provider;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getMaster() {
    return master;
  }

  public String getMasterScheme() {
    URI uri = URI.create(master);
    return uri.getScheme();
  }

  public String getMasterHost() {
    URI uri = URI.create(master);
    String host = uri.getHost();
    int port = uri.getPort();
    return port > 0 ? host + ":" + port : host;
  }

  public String getClusterType() {
    if (null == master) {
      return "";
    }
    return master.split("://")[0].toLowerCase(Locale.ROOT);
  }

  public void setMaster(String master) {
    this.master = master;
  }

  public String getWebUI() {
    return webUI;
  }

  public void setWebUI(String webUI) {
    this.webUI = webUI;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getServerName() {
    return serverName;
  }

  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public List<String> getConf() {
    return conf;
  }

  public void setConf(List<String> conf) {
    this.conf = conf;
  }

  public double getMaxUsage() {
    return maxUsage;
  }

  public void setMaxUsage(double maxUsage) {
    this.maxUsage = maxUsage;
  }

  public boolean isScalable() {
    return scalable;
  }

  public void setScalable(boolean scalable) {
    this.scalable = scalable;
  }

  public List<String> getNeedAccount() {
    return needAccount;
  }

  public void setNeedAccount(List<String> needAccount) {
    this.needAccount = needAccount;
  }

  public boolean isNeedComputeUsage() {
    return needComputeUsage;
  }

  public void setNeedComputeUsage(boolean needComputeUsage) {
    this.needComputeUsage = needComputeUsage;
  }

  public List<Instance> getInstances() {
    return instances;
  }

  public void setInstances(List<Instance> instances) {
    this.instances = instances;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public List<String> getActiveCommands() {
    return activeCommands;
  }

  public void setActiveCommands(List<String> activeCommands) {
    this.activeCommands = activeCommands;
  }

  public Commands.Command getCommand() {
    return command;
  }

  public void setCommand(Commands.Command command) {
    this.command = command;
  }

  public double getUsage() {
    return usage;
  }

  public void setUsage(double usage) {
    this.usage = usage;
  }

  public String getConfigPath() {
    return configPath;
  }

  public void setConfigPath(String configPath) {
    this.configPath = configPath;
  }

  public String getSparkConfigPath() {
    return Paths.get(configPath, SPARK_CONFIG_PATH, id).toString();
  }

  public String getSparkConfigFilePath() {
    return Paths.get(getSparkConfigPath(), "spark-defaults.conf").toString();
  }

  public String getKubeConfigFilePath() {
    return Paths.get(configPath, KUBERNETES_CONFIG_PATH, id, "kubeconfig").toString();
  }

  public String getKubernetesClusterConfigFilePath() {
    return Paths.get(configPath, KUBERNETES_CONFIG_PATH, id, "cluster_config.json").toString();
  }

  public String getYarnClusterConfigPath() {
    return Paths.get(configPath, YARN_CONFIG_PATH, id).toString();
  }

  @Override
  public int compareTo(@NotNull Cluster o) {
    return o.priority - priority;
  }

  @Override
  public String toString() {
    return new Gson().toJson(this);
  }

  @Override
  public Cluster clone() {
    try {
      return (Cluster) super.clone();
    } catch (CloneNotSupportedException e) {
      LOG.error("Cluster clone failed", e);
      return null;
    }
  }

  public static class Instance {
    @SerializedName("instance_type")
    private String type;

    @SerializedName("allocatable_cpu")
    private String cpu;

    @SerializedName("allocatable_memory")
    private String memory;

    @SerializedName("max_num")
    private int maxNum;

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getCpu() {
      return cpu;
    }

    public void setCpu(String cpu) {
      this.cpu = cpu;
    }

    public String getMemory() {
      return memory;
    }

    public void setMemory(String memory) {
      this.memory = memory;
    }

    public int getMaxNum() {
      return maxNum;
    }

    public void setMaxNum(int maxNum) {
      this.maxNum = maxNum;
    }

    @Override
    public String toString() {
      return "{"
          + "type='"
          + type
          + '\''
          + ", cpu='"
          + cpu
          + '\''
          + ", memory='"
          + memory
          + '\''
          + ", maxNum="
          + maxNum
          + '}';
    }
  }
}
