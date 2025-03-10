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

package org.apache.kyuubi.util;

import com.google.gson.Gson;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

public class YamlLoader {

  public static <T> T load(String yamlFilePath, Class<T> cls) throws FileNotFoundException {
    Yaml yaml = new Yaml();
    Map<String, Object> obj = yaml.load(new FileReader(yamlFilePath));
    Gson gson = new Gson();
    return new Gson().fromJson(gson.toJsonTree(obj).getAsJsonObject(), cls);
  }
}
