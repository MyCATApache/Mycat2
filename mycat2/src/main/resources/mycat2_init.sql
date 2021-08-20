create database `mycat`;
use `mycat`;

drop table if exists `xa_log`;

CREATE TABLE `xa_log` (
  `xid` bigint(20) NOT NULL,
  PRIMARY KEY (`xid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists `config`;

CREATE TABLE `config` (
  `key` varchar(22) NOT NULL,
  `value` text,
  `version` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists `replica_log`;

CREATE TABLE `replica_log` (
  `name` varchar(22) DEFAULT NULL,
  `dsNames` text,
  `time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


drop table if exists `spm_plan`;

CREATE TABLE `spm_plan` (
  `id` bigint(22) NOT NULL,
  `sql` longtext,
  `rel` longtext,
  `baseline_id` bigint(22) DEFAULT NULL,
  KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists `spm_baseline`;

CREATE TABLE `spm_baseline` (
  `id` bigint(22) NOT NULL,
  `fix_plan_id` bigint(22) DEFAULT NULL,
  `constraint` longtext CHARACTER SET utf8mb4 NOT NULL,
  `extra_constraint` longtext,
  PRIMARY KEY (`id`),
  UNIQUE KEY `constraint_index` (`constraint`(22)),
  KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists `sql_log`;

CREATE TABLE `sql_log` (
  `instanceId` bigint(20) DEFAULT NULL,
  `user` varchar(64) DEFAULT NULL,
  `connectionId` bigint(20) DEFAULT NULL,
  `ip` varchar(22) DEFAULT NULL,
  `port` bigint(20) DEFAULT NULL,
  `traceId` varchar(22) NOT NULL,
  `hash` varchar(22) DEFAULT NULL,
  `sqlType` varchar(22) DEFAULT NULL,
  `sql` longtext,
  `transactionId` varchar(22) DEFAULT NULL,
  `sqlTime` bigint(20) DEFAULT NULL,
  `responseTime` datetime DEFAULT NULL,
  `affectRow` int(11) DEFAULT NULL,
  `result` tinyint(1) DEFAULT NULL,
  `externalMessage` tinytext,
  PRIMARY KEY (`traceId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

drop table if exists `analyze_table`;

CREATE TABLE `analyze_table` (
  `table_rows` bigint(20) NOT NULL,
  `name` varchar(64) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;