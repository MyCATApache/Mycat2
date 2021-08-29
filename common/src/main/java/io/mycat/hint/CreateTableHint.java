/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hint;

import io.mycat.config.*;
import io.mycat.util.JsonUtil;

import java.text.MessageFormat;
import java.util.List;

public class CreateTableHint extends HintBuilder {
    private CreateTableConfig config;

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
            ShardingFunction shardingFuntion
    ) {
        return createSharding(schemaName, tableName,
                ShardingTableConfig.builder()
                        .createTableSQL(createTableSql)
                        .function(shardingFuntion)
                        .partition(dataNodes)
                        .build());
    }

    public static String createSharding(
            String schemaName,
            String tableName,
            ShardingTableConfig shadingTable) {
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setShardingTable(shadingTable);
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
        config.setBroadcast(dataNodes);
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
}