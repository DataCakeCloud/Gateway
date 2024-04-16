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

import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.util.YamlLoader;

public class Selectors {

  private List<Selector> selectors;
  private final Map<ClusterSelectorType, ClusterSelector> clusterSelectors = new HashMap<>();

  public static Selectors load(String selectorConfigFile) throws Exception {
    if (StringUtils.isBlank(selectorConfigFile)) {
      throw new IllegalArgumentException(
          "Load selectors failed reason selectorConfigFile is empty.");
    }
    Selectors s = YamlLoader.load(selectorConfigFile, Selectors.class);
    if (null == s || null == s.selectors || s.selectors.isEmpty()) {
      throw new IllegalArgumentException(
          "Load selectors failed, selectorConfigFile is [" + selectorConfigFile + "].");
    }

    for (Selector slt : s.selectors) {
      ClusterSelector cs = (ClusterSelector) Class.forName(slt.getClazz()).newInstance();
      s.clusterSelectors.put(cs.getType(), cs);
    }
    return s;
  }

  public List<Selector> getSelectors() {
    return selectors;
  }

  public void setSelectors(List<Selector> selectors) {
    this.selectors = selectors;
  }

  public boolean isEmpty() {
    return clusterSelectors.isEmpty();
  }

  public ClusterSelector getSelector(String name) {
    if (StringUtils.isBlank(name)) {
      return null;
    }
    return clusterSelectors.get(ClusterSelectorType.valueOf(name.toUpperCase()));
  }

  public ClusterSelector getSelector(ClusterSelectorType type) {
    return clusterSelectors.get(type);
  }

  @Override
  public String toString() {
    return "{" + "selectors=" + selectors + ", clusterSelectors=" + clusterSelectors + '}';
  }

  public static class Selector {
    private String name;

    @SerializedName("class")
    private String clazz;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getClazz() {
      return clazz;
    }

    public void setClazz(String clazz) {
      this.clazz = clazz;
    }

    @Override
    public String toString() {
      return "{" + "name='" + name + '\'' + ", clazz='" + clazz + '\'' + '}';
    }
  }
}
