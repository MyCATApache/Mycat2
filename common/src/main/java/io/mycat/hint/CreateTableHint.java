package io.mycat.hint;

import io.mycat.config.*;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.List;

public class CreateTableHint extends HintBuilder {
    private CreateTableConfig config;

    public void setConfig(CreateTableConfig config) {
        this.config = config;
    }

    @Override
    public String getCmd() {
        return "createTable";
    }

    @Override
    public String build() {
        return MessageFormat.format("/*+ mycat:{0}{1} */;",
                getCmd(),
                JsonUtil.toJson(config));
    }

    public static String create(
            CreateTableConfig config) {
        CreateTableHint createDataSourceHint = new CreateTableHint();
        createDataSourceHint.setConfig(config);
        return createDataSourceHint.build();
    }

    public static String createSharding(
            String schemaName,
            String tableName,
            String createTableSql,
            ShardingBackEndTableInfoConfig dataNodes,
            ShardingFuntion shardingFuntion
    ) {
        return createSharding(schemaName, tableName,
                ShardingTableConfig.builder()
                        .createTableSQL(createTableSql)
                        .function(shardingFuntion)
                        .dataNode(dataNodes)
                        .build());
    }

    public static String createSharding(
            String schemaName,
            String tableName,
            ShardingTableConfig shadingTable) {
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setShadingTable(shadingTable);
        createTableConfig.setSchemaName(schemaName);
        createTableConfig.setTableName(tableName);
        return create(createTableConfig);
    }

    public static String createGlobal(String schemaName,
                                      String tableName,
                                      String createTableSQL,
                                      List<GlobalBackEndTableInfoConfig> dataNodes) {
        GlobalTableConfig config = new GlobalTableConfig();
        config.setCreateTableSQL(createTableSQL);
        config.setDataNodes(dataNodes);
        return createGlobal(schemaName, tableName, config);
    }

    public static String createGlobal(
            String schemaName,
            String tableName,
            GlobalTableConfig tableConfig) {
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setGlobalTable(tableConfig);
        createTableConfig.setSchemaName(schemaName);
        createTableConfig.setTableName(tableName);
        return create(createTableConfig);
    }

    public static String createNormal(
            String schemaName,
            String tableName,
            String createTableSql,
            String targetName) {
        NormalTableConfig normalTableConfig = NormalTableConfig.create(schemaName, tableName, createTableSql, targetName);
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setNormalTable(normalTableConfig);
        createTableConfig.setSchemaName(schemaName);
        createTableConfig.setTableName(tableName);
        return create(createTableConfig);
    }

    public static String createNormal(
            String schemaName,
            String tableName,
            NormalTableConfig tableConfig) {
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setNormalTable(tableConfig);
        createTableConfig.setSchemaName(schemaName);
        createTableConfig.setTableName(tableName);
        return create(createTableConfig);
    }
}