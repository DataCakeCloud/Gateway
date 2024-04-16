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

import java.util.ArrayList;
import java.util.List;
import org.apache.kyuubi.cluster.Cluster;
import org.apache.kyuubi.cluster.Clusters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KubernetesClusters extends Clusters {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusters.class);

  public void computeUsage(List<Cluster> clusters) {
    for (Cluster cluster : clusters) {
      try {
        if (cluster.isNeedComputeUsage()) {
          KubernetesClusterClient client =
              KubernetesClusterClientFactory.newKubernetesClient(cluster, null);
          cluster.setUsage(client.usagePercent());
        } else {
          cluster.setUsage(1);
        }
      } catch (Exception e) {
        LOG.error("KubernetesClusters compute usage for cluster failed, cluster: [{}].", cluster);
      }
    }
  }

  public List<Cluster> filterOverloadCluster(List<Cluster> clusters) {
    List<Cluster> nonOverload = new ArrayList<>();
    for (Cluster cluster : clusters) {
      if (cluster.getUsage() < cluster.getMaxUsage()) {
        nonOverload.add(cluster);
      }
    }
    return nonOverload;
  }
}
