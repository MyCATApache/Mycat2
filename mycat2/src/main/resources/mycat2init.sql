CREATE DATABASE IF NOT EXISTS `mycat`;
USE `mycat`;
DROP TABLE IF EXISTS `analyze_table`;
CREATE TABLE `analyze_table` (
	`table_rows` bigint(20) NOT NULL,
	`name` varchar(64) NOT NULL
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
DROP TABLE IF EXISTS `config`;
CREATE TABLE `config` (
	`key` varchar(22) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
	`value` longtext,
	`version` bigint(20) DEFAULT NULL,
	`secondKey` longtext,
	`deleted` tinyint(1) DEFAULT '0'
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
DROP TABLE IF EXISTS `replica_log`;
CREATE TABLE `replica_log` (
	`name` varchar(22) DEFAULT NULL,
	`dsNames` text,
	`time` datetime DEFAULT NULL
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
DROP TABLE IF EXISTS `spm_baseline`;
CREATE TABLE `spm_baseline` (
	`id` bigint(22) NOT NULL AUTO_INCREMENT,
	`fix_plan_id` bigint(22) DEFAULT NULL,
	`constraint` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
	`extra_constraint` longtext,
	PRIMARY KEY (`id`),
	UNIQUE KEY `constraint_index` (`constraint`(22)),
	KEY `id` (`id`)
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
DROP TABLE IF EXISTS `spm_plan`;
CREATE TABLE `spm_plan` (
	`id` bigint(22) NOT NULL AUTO_INCREMENT,
	`sql` longtext,
	`rel` longtext,
	`baseline_id` bigint(22) DEFAULT NULL,
	KEY `id` (`id`)
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
DROP TABLE IF EXISTS `sql_log`;
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
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
DROP TABLE IF EXISTS `variable`;
CREATE TABLE `variable` (
	`name` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
	`value` varchar(22) DEFAULT NULL,
	PRIMARY KEY (`name`)
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
DROP TABLE IF EXISTS `xa_log`;
CREATE TABLE `xa_log` (
	`xid` bigint(20) NOT NULL,
	PRIMARY KEY (`xid`)
) ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
