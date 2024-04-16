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

package org.apache.kyuubi.authorization;

import io.lakecat.probe.model.SqlInfo;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.cluster.config.LakecatConfig;

public class AuthorizationManager {

  private final SqlAuth sqlAuth;

  public static AuthorizationManager load(LakecatConfig config) {
    return new AuthorizationManager(config);
  }

  AuthorizationManager(LakecatConfig config) {
    sqlAuth = new SqlAuth(config);
  }

  public static String format(String sql) {
    if (StringUtils.isBlank(sql)) {
      return null;
    }
    List<String> formatter =
        Arrays.stream(sql.split("\n"))
            .filter(a -> !a.trim().toLowerCase(Locale.ROOT).startsWith("--"))
            .collect(Collectors.toList());
    Iterator<String> it = formatter.iterator();
    while (it.hasNext()) {
      String line = it.next().trim().toLowerCase(Locale.ROOT);
      if (StringUtils.isEmpty(line) || line.startsWith("set ") || line.startsWith("add ")) {
        it.remove();
      } else {
        break;
      }
    }
    if (formatter.isEmpty()) {
      return null;
    }
    return String.join("\n", formatter);
  }

  public SqlInfo auth(
      String sql,
      String userName,
      String tenant,
      String catalog,
      String database,
      SqlAuth.Engine engineType)
      throws Exception {
    String formatter = format(sql);
    if (null == formatter) {
      return null;
    }
    return sqlAuth.auth(formatter, userName, tenant, catalog, database, engineType);
  }

  public void pushSqlInfo(
      SqlInfo sqlInfo,
      String sql,
      String taskId,
      String userName,
      String group,
      String tenant,
      String catalog,
      String database,
      SqlAuth.Engine engineType)
      throws Exception {
    sqlAuth.pushSqlInfo(
        sqlInfo, sql, taskId, userName, group, tenant, catalog, database, engineType);
  }

  public String genCatalog(String tenant, String provider, String region) {
    return sqlAuth.getLcCatalog(tenant, provider, region);
  }
}
