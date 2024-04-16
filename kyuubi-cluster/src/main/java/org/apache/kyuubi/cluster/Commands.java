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

import com.google.gson.annotations.SerializedName;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.util.YamlLoader;

public class Commands {

  private List<Command> commands;

  public static Commands load(String commandConfigFile) throws Exception {
    if (StringUtils.isBlank(commandConfigFile)) {
      throw new IllegalArgumentException("Load commands failed reason config file is empty.");
    }
    Commands cmd = YamlLoader.load(commandConfigFile, Commands.class);
    if (null == cmd || null == cmd.commands || cmd.commands.isEmpty()) {
      throw new IllegalArgumentException(
          "Load commands failed, commandConfigFile is [" + commandConfigFile + "].");
    }
    return cmd;
  }

  public List<Command> getCommands() {
    return commands;
  }

  public void setCommands(List<Command> commands) {
    this.commands = commands;
  }

  public boolean isEmpty() {
    return null == commands || commands.isEmpty();
  }

  public static class Command {

    private String id;
    private String name;
    private String version;
    private String command;

    private List<String> tags;
    private List<String> env;
    private List<String> conf;

    @SerializedName("active_clusters")
    private List<String> activeClusters;

    public Command() {}

    public Command(String id, String name, String version, String command) {
      this.id = id;
      this.name = name;
      this.version = version;
      this.command = command;
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

    public String getVersion() {
      return version;
    }

    public void setVersion(String version) {
      this.version = version;
    }

    public String getCommand() {
      return command;
    }

    public void setCommand(String command) {
      this.command = command;
    }

    public List<String> getTags() {
      return tags;
    }

    public void setTags(List<String> tags) {
      this.tags = tags;
    }

    public List<String> getEnv() {
      return env;
    }

    public void setEnv(List<String> env) {
      this.env = env;
    }

    public List<String> getConf() {
      return conf;
    }

    public void setConf(List<String> conf) {
      this.conf = conf;
    }

    public List<String> getActiveClusters() {
      return activeClusters;
    }

    public void setActiveClusters(List<String> activeClusters) {
      this.activeClusters = activeClusters;
    }
  }

  @Override
  public String toString() {
    return commands.toString();
  }
}
