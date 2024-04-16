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

package org.apache.kyuubi.spark.default.db

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.execution.SparkSqlParser

class AddDefaultDbNameSqlParser(sparkSession: SparkSession) extends SparkSqlParser {

  override protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    val dbName = sparkSession.conf.getOption("spark.sql.default.dbName")
    if (dbName.isDefined) {
      sparkSession.sessionState.catalogManager.setCurrentNamespace(Array(dbName.get))
      log.info(s"current catalog Name: " +
        s"${sparkSession.sessionState.catalogManager.currentCatalog.name()}, " +
        s"current namespace: " +
        s"${sparkSession.sessionState.catalogManager.currentNamespace.mkString(".")}")
    }
    super.parse(command)(toResult)
  }
}
