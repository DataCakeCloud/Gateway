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

package org.apache.kyuubi.k8s;

import io.kubernetes.client.PodLogs;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.ApiResponse;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.Cluster;
import org.apache.kyuubi.util.AutoRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClusterClient {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterClient.class);

  private static final String KEY_CPU = "cpu";
  private static final String KEY_MEM = "memory";
  private static final List<String> VALID_STATUS = Arrays.asList("Running", "Pending");

  public static class KubernetesResource {
    public BigDecimal cpu = BigDecimal.ZERO;
    public BigDecimal memory = BigDecimal.ZERO;

    public boolean empty() {
      return BigDecimal.ZERO.equals(cpu) && BigDecimal.ZERO.equals(memory);
    }

    @Override
    public String toString() {
      return "{cpu=" + cpu + ", memory=" + memory + '}';
    }
  }

  private final ApiClient client;
  private final CoreV1Api api;
  private final AppsV1Api appApi;
  private final Cluster cluster;

  KubernetesClusterClient(Cluster cluster) throws IOException {
    if (StringUtils.isEmpty(cluster.getId())) {
      throw new InvalidParameterException("clusterId is null");
    }
    Path kubeconfigPath = Paths.get(cluster.getKubeConfigFilePath());
    if (Files.notExists(kubeconfigPath)) {
      throw new IOException(
          "GetClusterByName from console failed, cluster: [" + cluster.getId() + "]");
    }
    this.cluster = cluster;
    client = Config.fromConfig(cluster.getKubeConfigFilePath());
    //    Configuration.setDefaultApiClient(client);
    api = new CoreV1Api(client);
    appApi = new AppsV1Api(client);
  }

  public ApiClient getApiClient() {
    return client;
  }

  public CoreV1Api getCoreApi() {
    return api;
  }

  public String getMaster() {
    return cluster.getMaster();
  }

  public Cluster getCluster() {
    return cluster;
  }

  public KubernetesResource getUsedResource() throws ApiException {
    KubernetesResource result = new KubernetesResource();
    /*
    V1PodList pods =
        api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null, false);
     */
    List<V1Pod> pods = KubernetesInformer.listPod(cluster.getId(), null, null, null);
    for (V1Pod pod : pods) {
      if (null == pod.getStatus()) {
        continue;
      }
      String status = pod.getStatus().getPhase();
      if (!VALID_STATUS.contains(status)) {
        continue;
      }
      if (null == pod.getSpec()) {
        continue;
      }
      List<V1Container> containers = pod.getSpec().getContainers();
      if (null == containers) {
        continue;
      }
      for (V1Container c : containers) {
        V1ResourceRequirements res = c.getResources();
        if (null == res) {
          continue;
        }
        Map<String, Quantity> map = res.getRequests();
        if (null == map) {
          continue;
        }
        if (null != map.get(KEY_CPU)) {
          result.cpu = result.cpu.add(map.get(KEY_CPU).getNumber());
        }
        if (null != map.get(KEY_MEM)) {
          result.memory = result.memory.add(map.get(KEY_MEM).getNumber());
        }
      }
    }
    return result;
  }

  public KubernetesResource getTotalResource() throws ApiException {
    KubernetesResource result = new KubernetesResource();
    List<Cluster.Instance> instances = cluster.getInstances();
    if (null != instances && !instances.isEmpty()) {
      for (Cluster.Instance instance : instances) {
        Quantity cpu = new Quantity(instance.getCpu());
        Quantity mem = new Quantity(instance.getMemory());
        result.cpu =
            result.cpu.add(cpu.getNumber().multiply(BigDecimal.valueOf(instance.getMaxNum())));
        result.memory =
            result.memory.add(mem.getNumber().multiply(BigDecimal.valueOf(instance.getMaxNum())));
      }
    } else if (!cluster.isScalable()) {
      // V1NodeList nodes = api.listNode(null, null, null, null, null, null, null, null, null,
      // false);
      List<V1Node> nodes = KubernetesInformer.listNode(cluster.getId());
      for (V1Node node : nodes) {
        V1NodeSpec spec = node.getSpec();
        if (null == spec) {
          continue;
        }
        if (null != spec.getUnschedulable() && spec.getUnschedulable()) {
          continue;
        }
        V1NodeStatus status = node.getStatus();
        if (null == status) {
          continue;
        }
        Map<String, Quantity> map = status.getAllocatable();
        if (null == map) {
          continue;
        }
        if (null != map.get(KEY_CPU)) {
          result.cpu = result.cpu.add(map.get(KEY_CPU).getNumber());
        }
        if (null != map.get(KEY_MEM)) {
          result.memory = result.memory.add(map.get(KEY_MEM).getNumber());
        }
      }
    }
    return result;
  }

  public double usagePercent() throws ApiException {
    KubernetesResource used = getUsedResource();
    KubernetesResource total = getTotalResource();
    if (total.empty()) {
      // 不能计算集群总资源时，使用率设置为 100%，会最后选择
      return 1;
    }
    double cpuUsedPercent =
        used.cpu
            .divide(
                total.cpu.equals(BigDecimal.ZERO) ? BigDecimal.ONE : total.cpu,
                4,
                RoundingMode.HALF_UP)
            .doubleValue();
    double memUsedPercent =
        used.memory
            .divide(
                total.memory.equals(BigDecimal.ZERO) ? BigDecimal.ONE : total.memory,
                4,
                RoundingMode.HALF_UP)
            .doubleValue();
    return Math.max(cpuUsedPercent, memUsedPercent);
  }

  public List<V1Pod> findPodByTag(String tag) throws ApiException {
    LOG.debug("FindPodByTag cluster: [{}], tag: [{}]", cluster.getId(), tag);
    //    V1PodList pods =
    //        api.listPodForAllNamespaces(null, null, null, tag, null, null, null, null, null,
    // false);
    //    return pods.getItems();
    return KubernetesInformer.listPod(cluster.getId(), null, null, tag);
  }

  public List<V1Deployment> findDeploymentByTag(String tag) throws ApiException {
    LOG.debug("FindDeploymentByTag cluster: [{}], tag: [{}]", cluster.getId(), tag);
    V1DeploymentList list =
        appApi.listDeploymentForAllNamespaces(
            null, null, null, tag, null, null, null, null, null, false);

    return list.getItems();
  }

  public void deletePodByTag(String tag) throws ApiException {
    LOG.info("DeletePodByTag cluster: [{}], tag: [{}]", cluster.getId(), tag);
    List<V1Pod> pods = findPodByTag(tag);
    if (pods.isEmpty()) {
      LOG.error("DeletePodByTag pod not found cluster: [{}], tag: [{}]", cluster.getId(), tag);
      throw new ApiException(String.format("Pod not found by this tag: [%s]", tag));
    }
    LOG.debug("DeletePodByTag pod size [{}]", pods.size());
    try {
      for (V1Pod pod : pods) {
        AutoRetry.executeWithRetry(
            () -> {
              ApiResponse<V1Pod> resp =
                  api.deleteNamespacedPodWithHttpInfo(
                      Objects.requireNonNull(pod.getMetadata()).getName(),
                      pod.getMetadata().getNamespace(),
                      null,
                      null,
                      null,
                      null,
                      null,
                      null);
              LOG.debug(
                  "DeletePodByTag tag: [{}], podName: [{}], deletePodResp:{}",
                  pods.size(),
                  pod.getMetadata().getName(),
                  resp);
              if (resp.getStatusCode() != 200) {
                LOG.error(
                    "DeletePodByTag pod failed cluster: [{}], tag: [{}], respCode: [{}]",
                    cluster.getId(),
                    tag,
                    resp.getStatusCode());
                throw new ApiException(
                    String.format(
                        "Pod delete failed by this tag: [%s], respCode: [%s]",
                        tag, resp.getStatusCode()));
              }
              return true;
            },
            3,
            3000,
            true);
      }
    } catch (Exception e) {
      LOG.error("DeletePodByTag pod failed cluster: [{}], tag: [{}]", cluster.getId(), tag, e);
    }
  }

  public void deleteDeploymentByTag(String tag) throws ApiException {
    LOG.info("DeleteDeploymentByTag cluster: [{}], tag: [{}]", cluster.getId(), tag);
    List<V1Deployment> deploys = findDeploymentByTag(tag);
    if (deploys.isEmpty()) {
      LOG.error(
          "DeleteDeploymentByTag deploy not found cluster: [{}], tag: [{}]", cluster.getId(), tag);
      throw new ApiException(String.format("Deployment not found by this tag: [%s]", tag));
    }
    LOG.debug("DeleteDeploymentByTag deploy size [{}]", deploys.size());
    try {
      for (V1Deployment deploy : deploys) {
        AutoRetry.executeWithRetry(
            () -> {
              ApiResponse<V1Status> resp =
                  appApi.deleteNamespacedDeploymentWithHttpInfo(
                      Objects.requireNonNull(deploy.getMetadata()).getName(),
                      deploy.getMetadata().getNamespace(),
                      null,
                      null,
                      null,
                      null,
                      null,
                      null);
              LOG.debug(
                  "DeleteDeploymentByTag tag: [{}], deployName: [{}], deleteDeploymentResp:{}",
                  deploys.size(),
                  deploy.getMetadata().getName(),
                  resp);
              if (resp.getStatusCode() != 200) {
                LOG.error(
                    "DeleteDeploymentByTag deploy failed cluster: [{}], tag: [{}], respCode: [{}]",
                    cluster.getId(),
                    tag,
                    resp.getStatusCode());
                throw new ApiException(
                    String.format(
                        "Pod delete failed by this tag: [%s], respCode: [%s]",
                        tag, resp.getStatusCode()));
              }
              return true;
            },
            3,
            3000,
            true);
      }
    } catch (Exception e) {
      LOG.error(
          "DeleteDeploymentByTag deploy failed cluster: [{}], tag: [{}]", cluster.getId(), tag, e);
    }
  }

  public InputStream getPodLogByTag(String tag) throws ApiException, IOException {
    LOG.info("GetPodLogByTag cluster: [{}], tag: [{}]", cluster.getId(), tag);
    PodLogs logs = new PodLogs(client);
    List<V1Pod> pods = findPodByTag(tag);
    if (pods.isEmpty()) {
      LOG.error(
          "GetPodLogByTag pod not found cluster: [{}], tag: [{}] failed", cluster.getId(), tag);
      throw new ApiException(String.format("Pod not found by this tag [%s]", tag));
    } else if (pods.size() > 1) {
      LOG.warn("GetPodLogByTag pod size > 1, we only use first pod, list.get(0).");
    }
    V1Pod pod = pods.get(0);
    return logs.streamNamespacedPodLog(pod);
  }

  public V1Service findService(String namespace, String tag, String name) throws ApiException {
    LOG.info("FindService namespace: [{}], tag: [{}], name: [{}]", namespace, tag, namespace);
    V1ServiceList list;
    if (StringUtils.isNotBlank(namespace)) {
      list =
          api.listNamespacedService(
              namespace, null, null, null, null, tag, null, null, null, null, null);
    } else {
      list =
          api.listServiceForAllNamespaces(
              null, null, null, tag, null, null, null, null, null, null);
    }
    for (V1Service svc : list.getItems()) {
      V1ObjectMeta meta = svc.getMetadata();
      if (null == meta) {
        continue;
      }
      if (name.equals(meta.getName())) {
        return svc;
      }
    }
    return null;
  }

  public String getLoadBalancerServiceExternalIp(String namespace, String app, String name)
      throws ApiException {
    LOG.info(
        "GetLoadBalancerServiceExternalIp namespace: [{}], app: [{}], name: [{}]",
        namespace,
        app,
        namespace);
    V1Service svc = findService(namespace, "app=" + app, name + "-rest");
    String empty = "";
    if (null == svc) {
      return empty;
    }
    V1ServiceStatus status = svc.getStatus();
    if (null == status) {
      return empty;
    }
    V1LoadBalancerStatus lb = status.getLoadBalancer();
    if (null == lb) {
      return empty;
    }
    List<V1LoadBalancerIngress> list = lb.getIngress();
    if (null == list) {
      return empty;
    }
    for (V1LoadBalancerIngress ingress : list) {
      String host = ingress.getHostname();
      if (StringUtils.isNotBlank(host)) {
        return host;
      }
    }

    for (V1LoadBalancerIngress ingress : list) {
      String ip = ingress.getIp();
      if (StringUtils.isNotBlank(ip)) {
        return ip;
      }
    }

    return empty;
  }

  @Override
  public String toString() {
    return cluster.getId();
  }
}
