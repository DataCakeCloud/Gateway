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

import org.apache.kyuubi.authentication.Group
import org.apache.kyuubi.cluster.Cluster
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ApplicationState.ApplicationState
import org.apache.kyuubi.engine.ResourceType.{POD, ResourceType}

trait ApplicationOperation {

  /**
   * Step for initializing the instance.
   */
  def initialize(conf: KyuubiConf): Unit

  /**
   * Step to clean up the instance
   */
  def stop(): Unit

  /**
   * Called before other method to do a quick skip
   *
   * @param clusterManager the underlying cluster manager or just local instance
   */
  def isSupported(clusterManager: Option[String]): Boolean

  /**
   * Kill the app/engine by the unique application tag
   *
   * @param tag the unique application tag for engine instance.
   *            For example,
   *            if the Hadoop Yarn is used, for spark applications,
   *            the tag will be preset via spark.yarn.tags
   * @return a message contains response describing how the kill process.
   *
   * @note For implementations, please suppress exceptions and always return KillResponse
   */
  def killApplicationByTag(tag: String): KillResponse

  def killApplicationByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None,
      resourceType: Option[ResourceType] = Some(POD)): KillResponse = {
    killApplicationByTag(tag)
  }

  /**
   * Get the engine/application status by the unique application tag
   *
   * @param tag the unique application tag for engine instance.
   * @return [[ApplicationInfo]]
   */
  def getApplicationInfoByTag(tag: String): ApplicationInfo

  def getApplicationInfoByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None,
      resourceType: Option[ResourceType] = Some(POD)): ApplicationInfo = {
    getApplicationInfoByTag(tag)
  }

  def getApplicationLogByTag(
      tag: String,
      cluster: Option[Cluster],
      group: Option[Group] = None): InputStream = {
    null
  }

}

object ApplicationState extends Enumeration {
  type ApplicationState = Value
  val PENDING, RUNNING, FINISHED, KILLED, FAILED, ZOMBIE, NOT_FOUND, UNKNOWN = Value

  def isFailed(state: ApplicationState): Boolean = state match {
    case FAILED => true
    case KILLED => true
    case _ => false
  }

  def isTerminated(state: ApplicationState): Boolean = {
    state match {
      case FAILED => true
      case KILLED => true
      case FINISHED => true
      case NOT_FOUND => true
      case _ => false
    }
  }

  def isPending(state: ApplicationState): Boolean = {
    state == PENDING
  }

  def isRunning(state: ApplicationState): Boolean = {
    state != NOT_FOUND && state != UNKNOWN
  }

  def isCompleted(state: ApplicationState): Boolean = {
    state match {
      case FAILED => true
      case KILLED => true
      case FINISHED => true
      case _ => false
    }
  }
}

object ResourceType extends Enumeration {
  type ResourceType = Value
  val POD, DEPLOYMENT, SERVICE = Value
}

case class ApplicationInfo(
    var id: String,
    name: String,
    state: ApplicationState,
    url: Option[String] = None,
    clusterId: Option[String] = None,
    error: Option[String] = None) {

  def toMap: Map[String, String] = {
    Map(
      "id" -> id,
      "name" -> name,
      "state" -> state.toString,
      "url" -> url.orNull,
      "clusterId" -> clusterId.orNull,
      "error" -> error.orNull)
  }

  override def toString: String = {
    String.join(", ", id, name, state.toString, clusterId.orNull)
  }
}

object ApplicationOperation {
  val NOT_FOUND = "APPLICATION_NOT_FOUND"
}
