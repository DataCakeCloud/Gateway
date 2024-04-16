-- the metadata table ddl

CREATE TABLE IF NOT EXISTS metadata (
  key_id bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'the auto increment key id',
  identifier varchar(36) NOT NULL COMMENT 'the identifier id, which is an UUID',
  session_type varchar(32) NOT NULL COMMENT 'the session type, SQL or BATCH',
  real_user varchar(255) NOT NULL COMMENT 'the real user',
  user_name varchar(255) NOT NULL COMMENT 'the user name, might be a proxy user',
  ip_address varchar(128) DEFAULT NULL COMMENT 'the client ip address',
  kyuubi_instance varchar(1024) NOT NULL COMMENT 'the kyuubi instance that creates this',
  state varchar(128) NOT NULL COMMENT 'the session state',
  resource varchar(1024) DEFAULT NULL COMMENT 'the main resource',
  class_name varchar(1024) DEFAULT NULL COMMENT 'the main class name',
  request_name varchar(256) DEFAULT NULL COMMENT 'the request name',
  request_conf mediumtext COMMENT 'the request config map',
  request_args mediumtext COMMENT 'the request arguments',
  create_time bigint(20) NOT NULL COMMENT 'the metadata create time',
  engine_type varchar(32) NOT NULL COMMENT 'the engine type',
  cluster_manager varchar(128) DEFAULT NULL COMMENT 'the engine cluster manager',
  engine_open_time bigint(20) DEFAULT NULL COMMENT 'the engine open time',
  engine_id varchar(128) DEFAULT NULL COMMENT 'the engine application id',
  engine_name mediumtext COMMENT 'the engine application name',
  engine_url varchar(1024) DEFAULT NULL COMMENT 'the engine tracking url',
  engine_state varchar(32) DEFAULT NULL COMMENT 'the engine application state',
  engine_error mediumtext COMMENT 'the engine application diagnose',
  end_time bigint(20) DEFAULT NULL COMMENT 'the metadata end time',
  peer_instance_closed tinyint(1) DEFAULT '0' COMMENT 'closed by peer kyuubi instance',
  PRIMARY KEY (key_id),
  UNIQUE KEY unique_identifier_index (identifier),
  KEY user_name_index (user_name),
  KEY engine_type_index (engine_type),
  KEY rn_ct (request_name,create_time),
  KEY cm_ct (cluster_manager,create_time),
  KEY ct (create_time),
  KEY et (end_time),
  KEY st_ct (session_type,create_time),
  KEY st_st_pic (session_type,state,peer_instance_closed),
  KEY s_ct (state,create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;


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

CREATE TABLE IF NOT EXISTS stat(
    id bigint PRIMARY KEY AUTO_INCREMENT COMMENT 'the auto increment id',
    identifier varchar(36) NOT NULL COMMENT 'session id or query id',
    username varchar(255) NOT NULL DEFAULT 'all' COMMENT 'the user name',
    region varchar(20) NOT NULL DEFAULT 'all' COMMENT 'the request region',
    engine_type varchar(32) NOT NULL DEFAULT 'all' COMMENT 'the engine type',
    cluster varchar(128) NOT NULL DEFAULT 'all' COMMENT 'the engine cluster name',
    input_size bigint NOT NULL DEFAULT 0 COMMENT 'scan data size, unit: byte',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'the blacklist create time',
    UNIQUE KEY unique_identifier_index (identifier)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;