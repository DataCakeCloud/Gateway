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

package org.apache.kyuubi.engine

import java.io.InputStream

import org.apache.kyuubi.Logging
import org.apache.kyuubi.authentication.Group
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState.RUNNING
import org.apache.kyuubi.engine.KubernetesApplicationOperation._
import org.apache.kyuubi.engine.ResourceType.{DEPLOYMENT, POD, ResourceType}
import org.apache.kyuubi.k8s.KubernetesClusterClientFactory

class KubernetesClusterApplicationOperation extends ApplicationOperation with Logging {

  private val appTag: String = {
    LABEL_KYUUBI_APP_KEY + "=" + LABEL_KYUUBI_APP_VALUE
  }

  override def initialize(conf: KyuubiConf): Unit = {
    // init cluster manager
    info("Start initializing kubernetes client")
  }

  override def isSupported(clusterManager: Option[String]): Boolean = clusterManager match {
    case Some(cm) => cm.toLowerCase.startsWith("k8s")
    case _ => false
  }

  def genTag(tag: String): String = {
    LABEL_KYUUBI_UNIQUE_KEY + "=" + tag
  }

  override def killApplicationByTag(tag: String): KillResponse = {
    null
  }

  override def killApplicationByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None,
      resourceType: Option[ResourceType] = Some(POD)): KillResponse =
    cluster match {
      case Some(clu) =>
        info(s"Deleting application info from Kubernetes cluster: [${clu.getId}], tag [$tag]")
        try {
          val client = KubernetesClusterClientFactory.newKubernetesClient(clu, appTag)
          if (null == client) {
            error(s"KillApplicationByTag client is null: [${clu.getId}]")
            return (false, s"KillApplicationByTag client is null: [${clu.getId}]")
          }
          resourceType match {
            case Some(POD) => client.deletePodByTag(genTag(tag))
            case Some(DEPLOYMENT) => client.deleteDeploymentByTag(genTag(tag))
            case _ =>
          }
        } catch {
          case e: Exception =>
            error(s"DeletePodByTag failed tag [$tag]: ", e)
            return (false, s"DeletePodByTag failed tag [$tag]: " + e.getMessage)
        }
        (true, "Succeeded")
      case None =>
        val err = "KillApplicationByTag failed, reason clusterId is null"
        error(err)
        (false, err)
    }

  override def getApplicationInfoByTag(tag: String): ApplicationInfo = {
    null
  }

  override def getApplicationInfoByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None,
      resourceType: Option[ResourceType] = Some(POD)): ApplicationInfo =
    cluster match {
      case Some(clu) =>
        debug(s"GetApplicationInfoByTag cluster:[${clu.getId}], tag:[$tag]")
        try {
          val client = KubernetesClusterClientFactory.newKubernetesClient(clu, appTag)
          if (null == client) {
            error(s"GetApplicationInfoByTag client is null: [${clu.getId}]")
            return ApplicationInfo(
              id = null,
              name = null,
              state = ApplicationState.NOT_FOUND,
              url = None,
              clusterId = Some(clu.getId),
              error = Some(s"GetApplicationInfoByTag client is null: [${clu.getId}]"))
          }

          resourceType match {
            case Some(POD) =>
              // find driver
              val podList = client.findPodByTag(genTag(tag))
              val size = podList.size()
              if (size != 1) {
                warn(s"Get Tag: [$tag] Driver Pod In Kubernetes size: [$size], we expect 1")
              }
              if (size > 0) {
                val pod = podList.get(0)
                var url: Option[String] = None
                if (null != pod.getStatus.getPodIP) {
                  url = Option(s"http://${pod.getStatus.getPodIP}:$SPARK_WEB_UI_PORT")
                }
                val app = ApplicationInfo(
                  // spark pods always tag label `spark-app-selector:<spark-app-id>`
                  id = Option(pod.getMetadata.getLabels.get(SPARK_APP_ID_LABEL)).getOrElse(tag),
                  name = pod.getMetadata.getName,
                  state = toApplicationState(pod.getStatus.getPhase),
                  url = url,
                  clusterId = Some(clu.getId),
                  error = Option(pod.getStatus.getReason))
                debug(s"Successfully got application info by [$tag]: $app")
                return app
              }
            case Some(DEPLOYMENT) =>
              val deployList = client.findDeploymentByTag(genTag(tag))
              val size = deployList.size()
              if (size != 1) {
                warn(s"Get Tag: [$tag] Deployment In Kubernetes size: [$size], we expect 1")
              }
              if (size > 0) {
                val deploy = deployList.get(0)
                val meta = deploy.getMetadata
                val url: Option[String] = Option(client.getLoadBalancerServiceExternalIp(
                  meta.getNamespace,
                  meta.getName,
                  meta.getName))
                val app = ApplicationInfo(
                  // spark pods always tag label `spark-app-selector:<spark-app-id>`
                  id = tag,
                  name = deploy.getMetadata.getName,
                  state = RUNNING,
                  url = url,
                  clusterId = Some(clu.getId),
                  error = None)
                debug(s"Successfully got application info by [$tag]: $app")
                return app
              }
            case _ =>
          }
          ApplicationInfo(
            id = null,
            name = null,
            state = ApplicationState.NOT_FOUND,
            url = None,
            clusterId = Some(clu.getId),
            error = None)

        } catch {
          case e: Exception =>
            error(s"Failed to get application with:")
            error(s"tag: [$tag], cluster: [${clu.getId}], due to ${e.getMessage}")
            ApplicationInfo(
              id = null,
              name = null,
              ApplicationState.NOT_FOUND,
              None,
              Some(clu.getId),
              Option(e.getMessage))
        }
      case None =>
        val err = "GetApplicationInfoByTag failed, reason cluster is null"
        error(err)
        ApplicationInfo(
          id = null,
          name = null,
          ApplicationState.NOT_FOUND,
          None,
          None,
          Option(err))
    }

  override def stop(): Unit = {
    KubernetesClusterClientFactory.shutdown()
  }

  override def getApplicationLogByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None): InputStream = {
    cluster match {
      case Some(clu) =>
        info(s"GetApplicationLogByTag cluster: [${clu.getId}], tag:[$tag]")
        try {
          val client = KubernetesClusterClientFactory.newKubernetesClient(clu, appTag)
          if (null == client) {
            error(s"GetApplicationLogByTag client is null: [${clu.getId}]")
            return null
          }
          client.getPodLogByTag(genTag(tag))
        } catch {
          case e: Exception =>
            error(s"Failed to get application log:", e)
            null
        }
      case None =>
        error("GetApplicationLogByTag failed, reason cluster is null")
        null
    }
  }

}
