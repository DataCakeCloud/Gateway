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

package org.apache.kyuubi.cluster;

import org.apache.kyuubi.authorization.AuthorizationManager;

public class TestSql {

  static void testFormatSql(String sql) {
    System.out.println(AuthorizationManager.format(sql));
  }

  public static void main(String[] args) {
    String sql =
        "-- a0\n"
            + "add jar \n"
            + "   set 11\n"
            + "-- a1\n"
            + "select xxx\n"
            + "create xxx\n"
            + "alter table xxx\n"
            + "-- a2\n"
            + "add partition (dt='20231217') location 'xxx'\n"
            + "-- a3";
    testFormatSql(sql);
  }
}
