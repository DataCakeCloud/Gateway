/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.jdbc.hive

import java.sql.Connection
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.rpc.thrift.TSessionHandle

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.engine.jdbc.connection.JdbcConnectionProvider
import org.apache.kyuubi.jdbc.hive.KyuubiConnection
import org.apache.kyuubi.session.SessionHandle

class HiveConnectionProvider extends JdbcConnectionProvider {

  override val name: String = classOf[HiveConnectionProvider].getSimpleName

  // override val driverClass: String = "org.apache.hive.jdbc.HiveDriver"
  override val driverClass: String = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"

  override def canHandle(providerClass: String): Boolean = {
    driverClass.equalsIgnoreCase(providerClass)
  }

  override def buildProperties(kyuubiConf: KyuubiConf): Properties = {
    val properties = super.buildProperties(kyuubiConf)
    // process lakecat.lineage
    val propKey = "hiveconf:hive.table.parameters.default"
    val sb = new StringBuffer
    sb.append(
      s"$LAKECAT_CATALOG_PROBE_FROM=${kyuubiConf.getOption(LAKECAT_CATALOG_PROBE_FROM)
          .getOrElse("gateway")}")
    sb.append(s",$LAKECAT_CATALOG_PROBE_CLUSTER=${kyuubiConf.get(KYUUBI_CLUSTER_NAME)}")
    sb.append(
      s",$LAKECAT_CATALOG_PROBE_REAL_USER=${kyuubiConf.getOption(KYUUBI_SESSION_USER_KEY)
          .getOrElse("")}")
    // TODO disable param
    // sb.append(s",$LAKECAT_CATALOG_PROBE_USER_GROUP=${kyuubiConf.get(KYUUBI_SESSION_TENANT)}")
    sb.append(
      s",$LAKECAT_CATALOG_PROBE_TASK_NAME=${kyuubiConf.getOption(LAKECAT_CATALOG_PROBE_TASK_NAME)
          .getOrElse("")}")
    sb.append(
      s",$LAKECAT_CATALOG_PROBE_TASK_ID=${kyuubiConf.getOption(LAKECAT_CATALOG_PROBE_TASK_ID)
          .getOrElse("")}")
    properties.setProperty(propKey, sb.toString)

    info(s"GetConnection properties: [$properties]")
    properties
  }

  override def getConnection(kyuubiConf: KyuubiConf): Connection = {
    if (kyuubiConf.get(KYUUBI_AUTHENTICATION_ENABLED)) {
      val principal = kyuubiConf.get(SERVER_PRINCIPAL).getOrElse("")
      val keytab = kyuubiConf.get(SERVER_KEYTAB).getOrElse("")
      val conf = new Configuration()
      conf.set("hadoop.security.authentication", "Kerberos")
      UserGroupInformation.setConfiguration(conf)
      info(s"GetConnection ugi loginUser: [$principal], keytab: [$keytab]")
      UserGroupInformation.loginUserFromKeytab(principal, keytab)
      info(s"GetConnection ugi curUser: [${UserGroupInformation.getCurrentUser.getUserName}]")
    }
    val conn = super.getConnection(kyuubiConf)
    conn match {
      case connection: KyuubiConnection =>
        val f = connection.getClass.getDeclaredField("sessHandle")
        f.setAccessible(true)
        val sessionHandle = SessionHandle(f.get(conn).asInstanceOf[TSessionHandle])
        info(s"Got the connection: sessionId[${sessionHandle.identifier.toString}]")
      case _ =>
    }
    conn
  }
}
