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

CREATE TABLE IF NOT EXISTS whitelist(
    id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment id',
    username varchar(255) NOT NULL DEFAULT 'all' COMMENT 'the user name',
    request_name varchar(256) NOT NULL DEFAULT 'all' COMMENT 'the request name',
    region varchar(20) NOT NULL DEFAULT 'all' COMMENT 'the request region',
    engine_type varchar(32) NOT NULL DEFAULT 'all' COMMENT 'the engine type',
    cluster varchar(128) NOT NULL DEFAULT 'all' COMMENT 'the engine cluster name',
    service varchar(128) NOT NULL DEFAULT 'all' COMMENT 'which service provide this record',
    request_conf text COMMENT 'the request config map',
    request_args text COMMENT 'the request arguments',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'the whitelist create time'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;


CREATE TABLE IF NOT EXISTS blacklist(
    id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment id',
    username varchar(255) NOT NULL DEFAULT 'all' COMMENT 'the user name',
    request_name varchar(256) NOT NULL DEFAULT 'all' COMMENT 'the request name',
    region varchar(20) NOT NULL DEFAULT 'all' COMMENT 'the request region',
    engine_type varchar(32) NOT NULL DEFAULT 'all' COMMENT 'the engine type',
    cluster varchar(128) NOT NULL DEFAULT 'all' COMMENT 'the engine cluster name',
    service varchar(128) NOT NULL DEFAULT 'all' COMMENT 'which service provide this record',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'the blacklist create time'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;

CREATE TABLE IF NOT EXISTS blacklist(
    id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment id',
    service varchar(128) NOT NULL DEFAULT 'all' COMMENT 'which service provide this record',
    bl tinyint NOT NULL DEFAULT 0 COMMENT 'whether enabled blacklist',
    wl tinyint NOT NULL DEFAULT 0 COMMENT 'whether enabled whitelist',
    action varchar(128) NOT NULL DEFAULT '' COMMENT 'enabled bwlist where action be happened',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'the service create time'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;
