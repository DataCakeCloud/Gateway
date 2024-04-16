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

import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.CallGeneratorParams;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesInformer {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesInformer.class);

  private static final Map<String, SharedInformerFactory> FACTORY_INFORMER_MAP = new HashMap<>();
  private static final Map<String, SharedIndexInformer<V1Pod>> POD_INFORMER_MAP = new HashMap<>();
  private static final Map<String, SharedIndexInformer<V1Node>> NODE_INFORMER_MAP = new HashMap<>();

  public static synchronized void registerKubernetesClient(
      KubernetesClusterClient client, String labels) {
    Cluster cluster = client.getCluster();
    if (FACTORY_INFORMER_MAP.containsKey(cluster.getId())) {
      return;
    }
    CoreV1Api coreV1Api = client.getCoreApi();
    ApiClient apiClient = client.getApiClient();
    OkHttpClient httpClient =
        apiClient.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
    apiClient.setHttpClient(httpClient);

    SharedInformerFactory factory = new SharedInformerFactory(apiClient);

    SharedIndexInformer<V1Pod> podInformer =
        factory.sharedIndexInformerFor(
            (CallGeneratorParams params) ->
                coreV1Api.listPodForAllNamespacesCall(
                    null,
                    null,
                    null,
                    labels,
                    null,
                    null,
                    params.resourceVersion,
                    null,
                    params.timeoutSeconds,
                    params.watch,
                    null),
            V1Pod.class,
            V1PodList.class);

    /*
    podInformer.addEventHandlerWithResyncPeriod(
        new ResourceEventHandler<V1Pod>() {
          @Override
          public void onAdd(V1Pod obj) {
            V1ObjectMeta meta = obj.getMetadata();
            if (null == meta) {
              LOG.error("PodInformer onAdd called but meta is null");
              return;
            }
            String podName = meta.getName();
            String label =
                meta.getLabels() != null ? meta.getLabels().get("kyuubi-unique-tag") : "null";
            LOG.info(podName + " added label[kyuubi-unique-tag=" + label + "]");
          }

          @Override
          public void onUpdate(V1Pod oldObj, V1Pod newObj) {
            V1ObjectMeta meta = newObj.getMetadata();
            if (null == meta) {
              LOG.error("PodInformer onUpdate called but meta is null");
              return;
            }
            String podName = meta.getName();
            String label =
                meta.getLabels() != null ? meta.getLabels().get("kyuubi-unique-tag") : "null";
            String oldState = oldObj.getStatus() != null ? oldObj.getStatus().getPhase() : "null";
            String newState = newObj.getStatus() != null ? newObj.getStatus().getPhase() : "null";

            LOG.info(
                podName
                    + " updated label[kyuubi-unique-tag="
                    + label
                    + "], state["
                    + oldState
                    + " => "
                    + newState
                    + "]");
          }

          @Override
          public void onDelete(V1Pod obj, boolean deletedFinalStateUnknown) {
            V1ObjectMeta meta = obj.getMetadata();
            if (null == meta) {
              LOG.error("PodInformer onDelete called but meta is null");
              return;
            }
            String podName = meta.getName();
            String label =
                meta.getLabels() != null ? meta.getLabels().get("kyuubi-unique-tag") : "null";
            LOG.info(
                podName
                    + " deleted label[kyuubi-unique-tag="
                    + label
                    + "], deletedFinalStateUnknown["
                    + deletedFinalStateUnknown
                    + "]");
          }
        },
        300000L);
     */

    POD_INFORMER_MAP.put(cluster.getId(), podInformer);
    /*
    if (!cluster.isScalable()) {
      SharedIndexInformer<V1Node> nodeInformer =
          factory.sharedIndexInformerFor(
              (CallGeneratorParams params) ->
                  coreV1Api.listNodeCall(
                      null,
                      null,
                      null,
                      null,
                      null,
                      null,
                      params.resourceVersion,
                      null,
                      params.timeoutSeconds,
                      params.watch,
                      null),
              V1Node.class,
              V1NodeList.class);
      NODE_INFORMER_MAP.put(cluster.getId(), nodeInformer);
    }
    */

    FACTORY_INFORMER_MAP.put(cluster.getId(), factory);
    factory.startAllRegisteredInformers();

    sync(podInformer, cluster.getId(), 300 * 1000L);
  }

  private static synchronized void sync(
      SharedInformer<?> informer, String clusterId, long timeout) {
    try {
      long start = System.currentTimeMillis();
      long sleepTime = Math.min(3000L, timeout);
      while (!informer.hasSynced()) {
        Thread.sleep(sleepTime);
        if (System.currentTimeMillis() - start > timeout) {
          LOG.error("Sync cluster timeout, cluster: [{}]", clusterId);
        }
      }
      LOG.info("Sync cluster finished, cluster: [{}]", clusterId);
    } catch (InterruptedException ignore) {
    }
  }

  public static synchronized void close() {
    for (SharedInformerFactory factory : FACTORY_INFORMER_MAP.values()) {
      factory.stopAllRegisteredInformers();
    }
  }

  public static List<V1Pod> listPod(String clusterId, String namespace, String name, String tag) {
    List<V1Pod> res = new ArrayList<>();
    SharedIndexInformer<V1Pod> informer = POD_INFORMER_MAP.get(clusterId);
    if (null == informer) {
      LOG.error("ListPod by tag[{}] empty reason clusterId not found", tag);
      return res;
    }
    Lister<V1Pod> podLister = new Lister<>(informer.getIndexer());

    if (StringUtils.isNotBlank(namespace) && StringUtils.isNotBlank(name)) {
      res.add(podLister.get(namespace + "/" + name));
    } else if (StringUtils.isNotBlank(name)) {
      for (V1Pod pod : podLister.list()) {
        if (null == pod.getMetadata()) {
          continue;
        }
        if (name.equals(pod.getMetadata().getName())) {
          res.add(pod);
          break;
        }
      }
    } else if (StringUtils.isNotBlank(tag)) {
      String[] kv = tag.split("=");
      if (kv.length < 2) {
        return res;
      }
      for (V1Pod pod : podLister.list()) {
        if (null == pod.getMetadata()) {
          continue;
        }
        Map<String, String> labels = pod.getMetadata().getLabels();
        if (null != labels && kv[1].equals(labels.get(kv[0]))) {
          res.add(pod);
        }
      }
    } else {
      res.addAll(podLister.list());
    }
    if (res.isEmpty()) {
      LOG.info(
          "ListAllPod by tag[{}]: {}",
          tag,
          podLister.list().stream()
              .map(a -> a.getMetadata() != null ? a.getMetadata().getName() : "null")
              .collect(Collectors.toList()));
    }
    return res;
  }

  public static List<V1Node> listNode(String clusterId) {
    SharedIndexInformer<V1Node> informer = NODE_INFORMER_MAP.get(clusterId);
    if (null == informer) {
      return Collections.emptyList();
    }
    Lister<V1Node> nodeLister = new Lister<>(informer.getIndexer());
    return nodeLister.list();
  }
}
