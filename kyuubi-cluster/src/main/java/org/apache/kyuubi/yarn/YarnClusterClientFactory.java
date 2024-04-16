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

package org.apache.kyuubi.yarn;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.HashMap;
import java.util.Map;
import org.apache.kyuubi.authentication.Group;
import org.apache.kyuubi.cluster.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnClusterClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClientFactory.class);

  private static final Map<String, YarnClusterClient> CLIENT_MAP = new HashMap<>();
  private static final Table<String, String, YarnClusterClient> CLIENT_TABLE =
      HashBasedTable.create();

  public static YarnClusterClient newYarnClient(Cluster cluster) throws Exception {
    if (CLIENT_MAP.containsKey(cluster.getId())) {
      return CLIENT_MAP.get(cluster.getId());
    } else {
      synchronized (YarnClusterClientFactory.class) {
        if (CLIENT_MAP.containsKey(cluster.getId())) {
          return CLIENT_MAP.get(cluster.getId());
        }
        YarnClusterClient client = new YarnClusterClient(cluster);
        CLIENT_MAP.put(cluster.getId(), client);
        return client;
      }
    }
  }

  public static YarnClusterClient newYarnClient(Cluster cluster, Group group) throws Exception {
    if (null == group) {
      return newYarnClient(cluster);
    }
    if (CLIENT_TABLE.contains(cluster.getId(), group.getName())) {
      return CLIENT_TABLE.get(cluster.getId(), group.getName());
    } else {
      synchronized (YarnClusterClientFactory.class) {
        if (CLIENT_TABLE.contains(cluster.getId(), group.getName())) {
          return CLIENT_TABLE.get(cluster.getId(), group.getName());
        }
        YarnClusterClient client = new YarnClusterClient(cluster, group);
        CLIENT_TABLE.put(cluster.getId(), group.getName(), client);
        return client;
      }
    }
  }

  public static void shutdown() {
    for (YarnClusterClient client : CLIENT_MAP.values()) {
      client.close();
    }
    for (YarnClusterClient client : CLIENT_TABLE.values()) {
      client.close();
    }
  }
}
