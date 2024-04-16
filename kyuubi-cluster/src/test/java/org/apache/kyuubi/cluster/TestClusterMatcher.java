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
import java.util.Arrays;
import java.util.List;
import org.apache.kyuubi.cluster.impl.hive.HiveClusters;
import org.apache.kyuubi.cluster.impl.spark.SparkClusters;
import org.apache.kyuubi.cluster.impl.trino.TrinoClusters;

public class TestClusterMatcher {

  public static void initSparkClusters() throws Exception {
    Gson gson = new Gson();
    SparkClusters sc = SparkClusters.load("cluster_config");
    System.out.println(gson.toJson(sc));
  }

  public static void initTrinoClusters() throws Exception {
    Gson gson = new Gson();
    TrinoClusters tc = TrinoClusters.load("cluster_config");
    System.out.println(gson.toJson(tc));
  }

  public static void initHiveClusters() throws Exception {
    Gson gson = new Gson();
    HiveClusters hc = HiveClusters.load("cluster_config");
    System.out.println(gson.toJson(hc));
  }

  public static void matchTags() throws Exception {
    Gson gson = new Gson();
    ClusterManager cm = ClusterManager.load("cluster_config");
    List<String> clusterTags = Arrays.asList("".split(","));
    // "name:aws,region:us-east-1".split(","));
    List<String> commandTags = Arrays.asList("".split(","));

    Cluster cluster = cm.selectCluster("spark", "1234", clusterTags, commandTags, null, true);

    System.out.println("matchCluster: " + gson.toJson(cluster));
  }

  public static void main(String[] args) throws Exception {
    initSparkClusters();
    initTrinoClusters();
    initHiveClusters();
    matchTags();
  }
}
