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

package org.apache.kyuubi.util

import java.nio.file.{Files, Paths}
import java.util.Locale

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.authentication.AuthenticationManager
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

object AuthUtil extends Logging {

  val AUTHENTICATION_METHOD_KERBEROS = "KERBEROS"

  def needAuthentication(engineType: String, engineProvider: String): Boolean = {
    if (null == engineType) {
      return false
    }

    engineType.toUpperCase(Locale.ROOT) match {
      case "HIVE" | "TRINO" | "SPARK" | "SPARK_SQL" | "FLINK" => true
      case "JDBC" =>
        if (null != engineProvider && engineProvider.toUpperCase(Locale.ROOT).startsWith("HIVE")) {
          return true
        }
        false
      case _ => false
    }
  }

  def authentication(
      conf: KyuubiConf,
      authenticationManager: Option[AuthenticationManager]): Unit = {
    authenticationManager match {
      case Some(am) =>
        if (!conf.get(KYUUBI_AUTHENTICATION_ENABLED)) {
          return
        }
        val engineType = conf.get(ENGINE_TYPE)
        val engineProvider = conf.get(ENGINE_JDBC_CONNECTION_PROVIDER).orNull
        // add authentication config
        if (AuthUtil.needAuthentication(engineType, engineProvider)) {
          val groupName = conf.get(KYUUBI_SESSION_GROUP)
          val groupId = conf.get(KYUUBI_SESSION_GROUP_ID)
          val keytabPrefix = conf.get(KYUUBI_SESSION_KEYTAB_PREFIX)
          val principalHost = conf.get(KYUUBI_SESSION_PRINCIPAL_HOST)
          val principalRealm = conf.get(KYUUBI_SESSION_PRINCIPAL_REALM)
          if (StringUtils.isEmpty(groupName) || StringUtils.isEmpty(groupId)) {
            throw new IllegalArgumentException(
              "authentication failed: groupName or groupId is empty")
          }
          val resPath = Paths.get("/tmp", groupName)
          if (Files.notExists(resPath)) {
            Files.createDirectories(resPath)
          }
          val filePath = Paths.get(resPath.toString, "krb5cc_0").toString
          val group =
            am.auth(groupId, groupName, keytabPrefix, principalHost, principalRealm, filePath)
          conf.set(AUTHENTICATION_METHOD, Seq(AUTHENTICATION_METHOD_KERBEROS))
          conf.set(SERVER_PRINCIPAL, group.getPrincipal)
          conf.set(SERVER_KEYTAB, group.getKeytab)
          conf.set(KYUUBI_KINIT_PATH, filePath)
        }
      case _ =>
    }
  }

  def authentication(
      batchType: String,
      batchConf: Map[String, String],
      conf: KyuubiConf,
      authenticationManager: Option[AuthenticationManager]): Unit = {
    authenticationManager match {
      case Some(am) =>
        if (!conf.get(KYUUBI_AUTHENTICATION_ENABLED)) {
          return
        }
        // add authentication config
        if (AuthUtil.needAuthentication(batchType, "")) {
          val groupName = batchConf.get(KYUUBI_SESSION_GROUP.key).orNull
          val groupId = batchConf.get(KYUUBI_SESSION_GROUP_ID.key).orNull
          val keytabPrefix = conf.get(KYUUBI_SESSION_KEYTAB_PREFIX)
          val principalHost = conf.get(KYUUBI_SESSION_PRINCIPAL_HOST)
          val principalRealm = conf.get(KYUUBI_SESSION_PRINCIPAL_REALM)
          if (StringUtils.isEmpty(groupName) || StringUtils.isEmpty(groupId)) {
            throw new IllegalArgumentException(
              "authentication failed: groupName or groupId is empty")
          }
          val resPath = Paths.get("/tmp", groupName)
          if (Files.notExists(resPath)) {
            Files.createDirectories(resPath)
          }
          val filePath = Paths.get(resPath.toString, "krb5cc_0").toString
          val group =
            am.auth(groupId, groupName, keytabPrefix, principalHost, principalRealm, filePath)
          conf.set(AUTHENTICATION_METHOD, Seq(AUTHENTICATION_METHOD_KERBEROS))
          conf.set(SERVER_PRINCIPAL, group.getPrincipal)
          conf.set(SERVER_KEYTAB, group.getKeytab)
          conf.set(KYUUBI_KINIT_PATH, filePath)
        }
      case _ =>
    }
  }
}
