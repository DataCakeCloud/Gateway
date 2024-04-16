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

package org.apache.kyuubi.cluster.selector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.kyuubi.cluster.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomClusterSelector implements ClusterSelector {

  private static final Logger LOG = LoggerFactory.getLogger(PriorityClusterSelector.class);

  @Override
  public ClusterSelectorType getType() {
    return ClusterSelectorType.RANDOM;
  }

  @Override
  public List<Cluster> select(List<Cluster> clusters) {
    LOG.debug("Select clusters before: {}", clusters);
    List<Cluster> res = new ArrayList<>();
    Random random = new Random();
    int r = random.nextInt(clusters.size());
    res.add(clusters.get(r));
    LOG.debug("Select clusters after: {}", res);
    return res;
  }
}
