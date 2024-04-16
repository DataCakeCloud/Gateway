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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.authentication.Group
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ResourceType.ResourceType
import org.apache.kyuubi.engine.YarnApplicationOperation.toApplicationState
import org.apache.kyuubi.yarn.YarnClusterClientFactory

class YarnClusterApplicationOperation extends ApplicationOperation with Logging {

  /**
   * Step for initializing the instance.
   */
  override def initialize(conf: KyuubiConf): Unit = {}

  /**
   * Step to clean up the instance
   */
  override def stop(): Unit = {
    YarnClusterClientFactory.shutdown()
  }

  /**
   * Called before other method to do a quick skip
   *
   * @param clusterManager the underlying cluster manager or just local instance
   */
  override def isSupported(clusterManager: Option[String]): Boolean = clusterManager match {
    case Some(cm) => cm.toLowerCase.startsWith("yarn")
    case _ => false
  }

  /**
   * Kill the app/engine by the unique application tag
   *
   * @param tag the unique application tag for engine instance.
   *            For example,
   *            if the Hadoop Yarn is used, for spark applications,
   *            the tag will be preset via spark.yarn.tags
   * @return a message contains response describing how the kill process.
   * @note For implementations, please suppress exceptions and always return KillResponse
   */
  override def killApplicationByTag(tag: String): (Boolean, String) = {
    (false, null)
  }

  override def killApplicationByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None,
      resourceType: Option[ResourceType] = None): (Boolean, String) =
    cluster match {
      case Some(clu) =>
        info(s"Kill application from yarn cluster: [${clu.getId}}], tag [$tag]")
        try {
          val client = YarnClusterClientFactory.newYarnClient(clu, group.orNull)
          client.deleteApplicationByTag(tag)
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

  /**
   * Get the engine/application status by the unique application tag
   *
   * @param tag the unique application tag for engine instance.
   * @return [[ApplicationInfo]]
   */
  override def getApplicationInfoByTag(tag: String): ApplicationInfo = {
    null
  }

  override def getApplicationInfoByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None,
      resourceType: Option[ResourceType] = None): ApplicationInfo =
    cluster match {
      case Some(clu) =>
        debug(s"GetApplicationInfoByTag cluster:[${clu.getId}], tag:[$tag]")
        try {
          val client = YarnClusterClientFactory.newYarnClient(clu, group.orNull)
          val reports = client.findApplicationByTag(tag)
          if (reports.isEmpty) {
            debug(s"Application with tag $tag not found")
            ApplicationInfo(id = null, name = null, state = ApplicationState.NOT_FOUND)
          } else {
            val report = reports.get(0)
            debug(s"Successfully got application info by $tag: $report")
            ApplicationInfo(
              id = report.getApplicationId.toString,
              name = report.getName,
              state = toApplicationState(
                report.getApplicationId.toString,
                report.getYarnApplicationState,
                report.getFinalApplicationStatus),
              url = Option(report.getTrackingUrl),
              error = Option(report.getDiagnostics))
          }
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
}
