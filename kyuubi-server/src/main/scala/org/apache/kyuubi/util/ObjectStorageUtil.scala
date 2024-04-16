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

import scala.util.control.NonFatal

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.storage.{ObjectStorage, ObjectStorageFactory, ObjectStorageType, ObjectStorageUrlParser}
import org.apache.kyuubi.util.JdbcUtils.warn

object ObjectStorageUtil {

  def close(c: AutoCloseable): Unit = {
    if (c != null) {
      try {
        c.close()
      } catch {
        case NonFatal(t) => warn(s"Error on closing", t)
      }
    }
  }

  def withCloseable[R, C <: AutoCloseable](c: C)(block: C => R): R = {
    try {
      block(c)
    } finally {
      close(c)
    }
  }

  def build(
      url: String,
      region: String,
      conf: KyuubiConf): ObjectStorage = {
    val parser = ObjectStorageUrlParser.parser(url)
    parser.getStorageType match {
      case ObjectStorageType.KS3 =>
        ObjectStorageFactory.create(
          parser.getStorageType,
          conf.get(KyuubiConf.KYUUBI_KS3_ENDPOINT),
          conf.get(KyuubiConf.KYUUBI_KS3_ACCESS_KEY),
          conf.get(KyuubiConf.KYUUBI_KS3_SECRET_KEY))
      case ObjectStorageType.OBS =>
        ObjectStorageFactory.create(
          parser.getStorageType,
          conf.get(KyuubiConf.KYUUBI_OBS_ENDPOINT),
          conf.get(KyuubiConf.KYUUBI_OBS_ACCESS_KEY),
          conf.get(KyuubiConf.KYUUBI_OBS_SECRET_KEY))
      case ObjectStorageType.S3 =>
        ObjectStorageFactory.create(
          parser.getStorageType,
          region)
      case ObjectStorageType.GS =>
        ObjectStorageFactory.create(
          parser.getStorageType,
          null)
      case ObjectStorageType.OSS =>
        ObjectStorageFactory.create(
          parser.getStorageType,
          conf.get(KyuubiConf.KYUUBI_OSS_ENDPOINT),
          conf.get(KyuubiConf.KYUUBI_OSS_ACCESS_KEY),
          conf.get(KyuubiConf.KYUUBI_OSS_SECRET_KEY))
      case _ =>
        throw new IllegalArgumentException(s"not support [${parser.getStorageType}] output")
    }
  }

}
