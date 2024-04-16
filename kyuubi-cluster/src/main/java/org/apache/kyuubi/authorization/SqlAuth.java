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

import io.lakecat.catalog.common.model.TableUsageProfile;
import io.lakecat.probe.EventProcessor;
import io.lakecat.probe.hive.HiveSqlInfoParser;
import io.lakecat.probe.model.SqlInfo;
import io.lakecat.probe.model.TableMetaInfo;
import io.lakecat.probe.spark.SparkSqlInfoParser;
import io.lakecat.probe.trino.TrinoSqlInfoParser;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kyuubi.cluster.config.LakecatConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlAuth {
  public enum Engine {
    TRINO,
    SPARK,
    HIVE,
    UNKNOWN
  }

  private static final Logger LOG = LoggerFactory.getLogger(SqlAuth.class);
  private final LakecatConfig lakecatConf;

  public SqlAuth(LakecatConfig lakecatConf) {
    this.lakecatConf = lakecatConf;
  }

  public SqlInfo auth(
      String sql,
      String username,
      String projectId,
      String catalog,
      String database,
      Engine engineType)
      throws Exception {
    String queryId = createQueryId(username, sql);
    Configuration conf = lakecatConf.getConf();
    conf.set("lakecat.client.userName", username);
    conf.set("lakecat.client.projectId", projectId);
    conf.set("lakecat.client.tenantName", projectId);
    /*
    LOG.info(
        "lakecat.host:{},lakecat.port:{}",
        conf.get("lakecat.client.host"),
        conf.get("lakecat.client.port"));
     */
    LOG.info(
        "AuthSql: [username:{},projectId:{},catalog:{},database:{},engineType:{}]",
        username,
        projectId,
        catalog,
        database,
        engineType);
    SqlInfo sqlInfo;
    try {
      if (Engine.SPARK == engineType) {
        conf.set("spark.sql.default.dbName", database);
        SparkSqlInfoParser parser = new SparkSqlInfoParser(conf);
        sqlInfo = parser.parse(sql, catalog);
      } else if (Engine.TRINO == engineType) {
        conf.set("trino.sql.default.dbName", database);
        TrinoSqlInfoParser parser = new TrinoSqlInfoParser(conf);
        sqlInfo = parser.parse(sql, catalog);
      } else {
        conf.set("hive.sql.default.dbName", database);
        HiveSqlInfoParser parser = new HiveSqlInfoParser(conf);
        sqlInfo = parser.parse(sql, catalog);
      }
    } catch (Exception e) {
      LOG.error("LakeCat Exception, queryId:{}", queryId);
      throw e;
    }

    return sqlInfo;
  }

  public void pushSqlInfo(
      SqlInfo sqlInfo,
      String sql,
      String taskId,
      String username,
      String group,
      String projectId,
      String catalog,
      String database,
      Engine engineType)
      throws Exception {

    if (null == sqlInfo) {
      return;
    }

    String queryId = createQueryId(username, sql);
    Configuration conf = lakecatConf.getConf();
    conf.set("lakecat.client.userName", username);
    conf.set("lakecat.client.projectId", projectId);
    conf.set("lakecat.client.tenantName", projectId);
    conf.set("catalog.probe.userGroup", group);
    conf.set("catalog.probe.taskId", taskId);
    LOG.info(
        "PushSql: [taskId:{},username:{},group:{},projectId:{},catalog:{},database:{},engineType:{},sqlInfo:{}]",
        taskId,
        username,
        group,
        projectId,
        catalog,
        database,
        engineType,
        sqlInfo);
    try {
      if (Engine.TRINO == engineType) {
        conf.set("trino.sql.default.dbName", database);
        // only push in trino
        try (EventProcessor eventProcessor = new EventProcessor(conf)) {
          List<TableUsageProfile> tableUsageProfiles = sqlInfo.getTableUsageProfiles();
          if (null != tableUsageProfiles && !tableUsageProfiles.isEmpty()) {
            TableMetaInfo metaInfo = new TableMetaInfo();
            metaInfo.setTableUsageProfiles(tableUsageProfiles);
            metaInfo.setTaskId(queryId);
            eventProcessor.pushEvent(metaInfo);
          }
        } catch (Exception e) {
          LOG.error("LakeCat Exception, queryId:{}", queryId);
          throw e;
        }
      } else if (Engine.HIVE == engineType) {
        conf.set("hive.sql.default.dbName", database);
        // disable change owner at here
        // HiveSqlInfoParser parser = new HiveSqlInfoParser(conf);
        // parser.alterTableOrDatabaseOwner(sqlInfo.getOperationObjects(), catalog, username,
        // group);
      } else if (Engine.SPARK == engineType) {
        conf.set("spark.sql.default.dbName", database);
      }
    } catch (Exception e) {
      LOG.error("LakeCat Exception, queryId:{}", queryId);
      throw e;
    }
  }

  private String createQueryId(String username, String sql) {
    String base = username + sql + System.currentTimeMillis();
    return DigestUtils.md5Hex(base.getBytes(StandardCharsets.UTF_8));
  }

  public String getLcCatalog(String projectId, String provider, String region) {
    if (Arrays.asList("shareit", "payment").contains(projectId)) {
      switch (region) {
        case "us-east-1":
          return "shareit_ue1";
        case "ap-southeast-1":
          return "shareit_sg1";
        case "ap-southeast-3":
          return "shareit_sg2";
      }
    }
    return provider + "_" + region;
  }
}
