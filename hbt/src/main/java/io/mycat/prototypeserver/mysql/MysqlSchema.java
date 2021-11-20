package io.mycat.prototypeserver.mysql;

import io.mycat.config.LogicSchemaConfig;
import io.mycat.config.NormalTableConfig;
import io.mycat.util.JsonUtil;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MysqlSchema {
    public static String role_edges = "CREATE TABLE mysql.role_edges (\n\t`FROM_HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',\n\t`FROM_USER` char(32) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',\n\t`TO_HOST` char(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',\n\t`TO_USER` char(32) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',\n\t`WITH_ADMIN_OPTION` enum('N', 'Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n\tPRIMARY KEY (`FROM_HOST`, `FROM_USER`, `TO_HOST`, `TO_USER`)\n) ENGINE = InnoDB CHARSET = utf8 COLLATE = utf8_bin STATS_PERSISTENT = 0 ROW_FORMAT = DYNAMIC COMMENT 'Role hierarchy and role grants'";
    public static String proc = "CREATE TABLE `mysql`.`proc` (\n  `db` varchar(64) DEFAULT NULL,\n  `name` varchar(64) DEFAULT NULL,\n  `type` enum('FUNCTION','PROCEDURE','PACKAGE', 'PACKAGE BODY'),\n  `specific_name` varchar(64) DEFAULT NULL,\n  `language` enum('SQL'),\n  `sql_data_access` enum('CONTAINS_SQL', 'NO_SQL', 'READS_SQL_DATA', 'MODIFIES_SQL_DATA'),\n  `is_deterministic` enum('YES','NO'),\n  `security_type` enum('INVOKER','DEFINER'),\n  `param_list` blob,\n  `returns` longblob,\n  `body` longblob,\n  `definer` varchar(141),\n  `created` timestamp,\n  `modified` timestamp,\n  `sql_mode` \tset('REAL_AS_FLOAT', 'PIPES_AS_CONCAT', 'ANSI_QUOTES', 'IGNORE_SPACE', 'IGNORE_BAD_TABLE_OPTIONS', 'ONLY_FULL_GROUP_BY', 'NO_UNSIGNED_SUBTRACTION', 'NO_DIR_IN_CREATE', 'POSTGRESQL', 'ORACLE', 'MSSQL', 'DB2', 'MAXDB', 'NO_KEY_OPTIONS', 'NO_TABLE_OPTIONS', 'NO_FIELD_OPTIONS', 'MYSQL323', 'MYSQL40', 'ANSI', 'NO_AUTO_VALUE_ON_ZERO', 'NO_BACKSLASH_ESCAPES', 'STRICT_TRANS_TABLES', 'STRICT_ALL_TABLES', 'NO_ZERO_IN_DATE', 'NO_ZERO_DATE', 'INVALID_DATES', 'ERROR_FOR_DIVISION_BY_ZERO', 'TRADITIONAL', 'NO_AUTO_CREATE_USER', 'HIGH_NOT_PRECEDENCE', 'NO_ENGINE_SUBSTITUTION', 'PAD_CHAR_TO_FULL_LENGTH', 'EMPTY_STRING_IS_NULL', 'SIMULTANEOUS_ASSIGNMENT'),\n  `comment` text,\n  `character_set_client` char(32),\n  `collation_connection` \tchar(32),\n  `db_collation` \tchar(32),\n  `body_utf8` \tlongblob,\n  `aggregate` \tenum('NONE', 'GROUP')\n) ";
    public static String innodb_index_stats = "CREATE TABLE mysql.innodb_index_stats (\n\t`database_name` varchar(64) COLLATE utf8_bin NOT NULL,\n\t`table_name` varchar(199) COLLATE utf8_bin NOT NULL,\n\t`index_name` varchar(64) COLLATE utf8_bin NOT NULL,\n\t`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n\t`stat_name` varchar(64) COLLATE utf8_bin NOT NULL,\n\t`stat_value` bigint(20) UNSIGNED NOT NULL,\n\t`sample_size` bigint(20) UNSIGNED DEFAULT NULL,\n\t`stat_description` varchar(1024) COLLATE utf8_bin NOT NULL,\n\tPRIMARY KEY (`database_name`, `table_name`, `index_name`, `stat_name`)\n) ENGINE = InnoDB CHARSET = utf8 COLLATE = utf8_bin STATS_PERSISTENT = 0 ROW_FORMAT = DYNAMIC";
    public static String innodb_table_stats = "CREATE TABLE mysql.innodb_table_stats (\n\t`database_name` varchar(64) COLLATE utf8_bin NOT NULL,\n\t`table_name` varchar(199) COLLATE utf8_bin NOT NULL,\n\t`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n\t`n_rows` bigint(20) UNSIGNED NOT NULL,\n\t`clustered_index_size` bigint(20) UNSIGNED NOT NULL,\n\t`sum_of_other_index_sizes` bigint(20) UNSIGNED NOT NULL,\n\tPRIMARY KEY (`database_name`, `table_name`)\n) ENGINE = InnoDB CHARSET = utf8 COLLATE = utf8_bin STATS_PERSISTENT = 0 ROW_FORMAT = DYNAMIC";
    public static String user = "CREATE TABLE mysql.`user` (\n" +
            "  `Host` char(60) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
            "  `User` char(32) COLLATE utf8_bin NOT NULL DEFAULT '',\n" +
            "  `Select_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Insert_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Update_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Delete_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Create_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Drop_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Reload_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Shutdown_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Process_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `File_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Grant_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `References_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Index_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Alter_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Show_db_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Super_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Create_tmp_table_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Lock_tables_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Execute_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Repl_slave_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Repl_client_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Create_view_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Show_view_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Create_routine_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Alter_routine_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Create_user_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Event_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Trigger_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Create_tablespace_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `ssl_type` enum('','ANY','X509','SPECIFIED') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',\n" +
            "  `ssl_cipher` blob NOT NULL,\n" +
            "  `x509_issuer` blob NOT NULL,\n" +
            "  `x509_subject` blob NOT NULL,\n" +
            "  `max_questions` int(11) unsigned NOT NULL DEFAULT '0',\n" +
            "  `max_updates` int(11) unsigned NOT NULL DEFAULT '0',\n" +
            "  `max_connections` int(11) unsigned NOT NULL DEFAULT '0',\n" +
            "  `max_user_connections` int(11) unsigned NOT NULL DEFAULT '0',\n" +
            "  `plugin` char(64) COLLATE utf8_bin NOT NULL DEFAULT 'caching_sha2_password',\n" +
            "  `authentication_string` text COLLATE utf8_bin,\n" +
            "  `password_expired` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `password_last_changed` timestamp NULL DEFAULT NULL,\n" +
            "  `password_lifetime` smallint(5) unsigned DEFAULT NULL,\n" +
            "  `account_locked` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Create_role_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Drop_role_priv` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',\n" +
            "  `Password_reuse_history` smallint(5) unsigned DEFAULT NULL,\n" +
            "  `Password_reuse_time` smallint(5) unsigned DEFAULT NULL,\n" +
            "  `Password_require_current` enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,\n" +
            "  `User_attributes` json DEFAULT NULL,\n" +
            "  PRIMARY KEY (`Host`,`User`)\n" +
            ") /*!50100 TABLESPACE `mysql` */ ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin STATS_PERSISTENT=0 COMMENT='Users and global privileges'";

    @SneakyThrows
    public static void main(String[] args) {


        Map<String, Map<String, NormalTableConfig>> normalTablesSet = Collections.emptyMap();


        HashMap<String, HashMap<String, String>> map = new HashMap<>();
        for (Map.Entry<String, Map<String, NormalTableConfig>> stringMapEntry : normalTablesSet.entrySet()) {
            String key = stringMapEntry.getKey();
            Map<String, NormalTableConfig> value = stringMapEntry.getValue();

            for (Map.Entry<String, NormalTableConfig> entry : stringMapEntry.getValue().entrySet()) {
                NormalTableConfig normalTableConfig = entry.getValue();
                String createTableSQL = normalTableConfig.getCreateTableSQL();
                String tableName = entry.getKey();
                String schemaName = key;

                HashMap<String, String> stringStringHashMap = map.computeIfAbsent(schemaName, s1 -> new HashMap<>());
                stringStringHashMap.put(tableName, createTableSQL);

            }
        }
        for (Map.Entry<String, HashMap<String, String>> stringHashMapEntry : map.entrySet()) {
            String key = stringHashMapEntry.getKey();
            HashMap<String, String> value = stringHashMapEntry.getValue();
            System.out.println("-----------------------------------");
            for (Map.Entry<String, String> stringStringEntry : value.entrySet()) {
                String s1 = JsonUtil.toJson(stringStringEntry.getValue());

                System.out.println("public static String " + stringStringEntry.getKey() + " = " + s1 + ";");
            }


        }


    }

    private static Map<String, NormalTableConfig> getStringNormalTableConfigMap(String path) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(path));
        String s = new String(bytes);
        LogicSchemaConfig logicSchemaConfig = JsonUtil.from(s, LogicSchemaConfig.class);
        Map<String, NormalTableConfig> normalTables1 = logicSchemaConfig.getNormalTables();
        return normalTables1;
    }
}
