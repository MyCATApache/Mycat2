package io.mycat;

import io.mycat.calcite.table.DualCustomTableHandler;
import io.mycat.config.CustomTableConfig;
import io.mycat.config.LogicSchemaConfig;
import io.mycat.config.NormalTableConfig;

import java.util.*;

public class MysqlMetadataManager extends MetadataManager {

    public MysqlMetadataManager(Map<String,LogicSchemaConfig> schemaConfigs, String prototype) {
        super(addMySQLSystemTable(schemaConfigs), prototype);
    }
    public static Map<String,LogicSchemaConfig> getMySQLSystemTables(){
        return addMySQLSystemTable(Collections.emptyMap());
    }

    public static Map<String,LogicSchemaConfig> addMySQLSystemTable(Map<String,LogicSchemaConfig> orginal) {
//        orginal = new HashMap<>(orginal);
//        Set<String> databases = new HashSet<>();
//        databases.add("information_schema");
//        databases.add("mysql");
//        databases.add("performance_schema");
//
//
//        for (String database : databases) {
//            if (!orginal.containsKey(database)){
//                LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
//                schemaConfig.setSchemaName(database);
//                schemaConfig.setTargetName("prototype");
//                orginal.put(database,schemaConfig);
//            }
//        }
//
//        ArrayList<LogicSchemaConfig> logicSchemaConfigs = new ArrayList<>();
//        addInnerTable(logicSchemaConfigs,"prototype");
//        for (LogicSchemaConfig logicSchemaConfig : logicSchemaConfigs) {
//            if (!orginal.containsKey(logicSchemaConfig.getSchemaName())){
//                orginal.put(logicSchemaConfig.getSchemaName(),logicSchemaConfig);
//            }
//        }
        return orginal;
    }

    private  static void addInnerTable(List<LogicSchemaConfig> schemaConfigs, String prototype) {
        String schemaName = "mysql";
        String targetName = "prototype";
        String tableName = "proc";

        LogicSchemaConfig logicSchemaConfig = schemaConfigs.stream()
                .filter(i -> schemaName.equals(i.getSchemaName()))
                .findFirst()
                .orElseGet(() -> {
                    LogicSchemaConfig config = new LogicSchemaConfig();
                    config.setSchemaName(schemaName);
                    config.setTargetName(prototype);
                    schemaConfigs.add(config);
                    return config;
                });


        Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
        normalTables.putIfAbsent(tableName, NormalTableConfig.create(schemaName, tableName,
                "CREATE TABLE `mysql`.`proc` (\n" +
                        "  `db` varchar(64) DEFAULT NULL,\n" +
                        "  `name` varchar(64) DEFAULT NULL,\n" +
                        "  `type` enum('FUNCTION','PROCEDURE','PACKAGE', 'PACKAGE BODY'),\n" +
                        "  `specific_name` varchar(64) DEFAULT NULL,\n" +
                        "  `language` enum('SQL'),\n" +
                        "  `sql_data_access` enum('CONTAINS_SQL', 'NO_SQL', 'READS_SQL_DATA', 'MODIFIES_SQL_DATA'),\n" +
                        "  `is_deterministic` enum('YES','NO'),\n" +
                        "  `security_type` enum('INVOKER','DEFINER'),\n" +
                        "  `param_list` blob,\n" +
                        "  `returns` longblob,\n" +
                        "  `body` longblob,\n" +
                        "  `definer` varchar(141),\n" +
                        "  `created` timestamp,\n" +
                        "  `modified` timestamp,\n" +
                        "  `sql_mode` \tset('REAL_AS_FLOAT', 'PIPES_AS_CONCAT', 'ANSI_QUOTES', 'IGNORE_SPACE', 'IGNORE_BAD_TABLE_OPTIONS', 'ONLY_FULL_GROUP_BY', 'NO_UNSIGNED_SUBTRACTION', 'NO_DIR_IN_CREATE', 'POSTGRESQL', 'ORACLE', 'MSSQL', 'DB2', 'MAXDB', 'NO_KEY_OPTIONS', 'NO_TABLE_OPTIONS', 'NO_FIELD_OPTIONS', 'MYSQL323', 'MYSQL40', 'ANSI', 'NO_AUTO_VALUE_ON_ZERO', 'NO_BACKSLASH_ESCAPES', 'STRICT_TRANS_TABLES', 'STRICT_ALL_TABLES', 'NO_ZERO_IN_DATE', 'NO_ZERO_DATE', 'INVALID_DATES', 'ERROR_FOR_DIVISION_BY_ZERO', 'TRADITIONAL', 'NO_AUTO_CREATE_USER', 'HIGH_NOT_PRECEDENCE', 'NO_ENGINE_SUBSTITUTION', 'PAD_CHAR_TO_FULL_LENGTH', 'EMPTY_STRING_IS_NULL', 'SIMULTANEOUS_ASSIGNMENT'),\n" +
                        "  `comment` text,\n" +
                        "  `character_set_client` char(32),\n" +
                        "  `collation_connection` \tchar(32),\n" +
                        "  `db_collation` \tchar(32),\n" +
                        "  `body_utf8` \tlongblob,\n" +
                        "  `aggregate` \tenum('NONE', 'GROUP')\n" +
                        ") ", targetName));

        LogicSchemaConfig mycat = schemaConfigs.stream().filter(i ->
                "mycat".equalsIgnoreCase(i.getSchemaName()))
                .findFirst().orElseGet(() -> {
                    LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
                    schemaConfig.setSchemaName("mycat");
                    schemaConfigs.add(schemaConfig);
                    return schemaConfig;
                });
        Map<String, CustomTableConfig> customTables = mycat.getCustomTables();

        customTables.computeIfAbsent("dual", (n) -> {
            CustomTableConfig tableConfig = CustomTableConfig.builder().build();
            tableConfig.setClazz(DualCustomTableHandler.class.getCanonicalName());
            tableConfig.setCreateTableSQL("create table mycat.dual(id int)");
            return tableConfig;
        });
    }

}
