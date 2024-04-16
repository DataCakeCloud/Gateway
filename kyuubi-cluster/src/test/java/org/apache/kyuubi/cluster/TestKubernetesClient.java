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

import io.kubernetes.client.openapi.models.V1Deployment;
import java.util.Arrays;
import java.util.List;
import org.apache.kyuubi.k8s.KubernetesClusterClient;
import org.apache.kyuubi.k8s.KubernetesClusterClientFactory;

public class TestKubernetesClient {

  public static void main(String[] args) throws Exception {
    ClusterManager cm = ClusterManager.load("cluster_config");
    List<String> clusterTags = Arrays.asList("".split(","));
    List<String> commandTags = Arrays.asList("".split(","));
    Cluster clu = cm.selectCluster("flink", "123", clusterTags, commandTags, null, false);
    KubernetesClusterClient client = KubernetesClusterClientFactory.newKubernetesClient(clu, null);
    System.out.println(client);

    client.deleteDeploymentByTag("app=flink-paimon");

    List<V1Deployment> list = client.findDeploymentByTag("app=flink-paimon");
    System.out.println(list.size());
    KubernetesClusterClientFactory.shutdown();
  }
}
