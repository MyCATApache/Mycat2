/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite.table;

import io.mycat.*;
import io.mycat.config.NormalTableConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.mycat.util.CreateTableUtils.createPhysicalTable;
import static io.mycat.util.CreateTableUtils.normalizeCreateTableSQLToMySQL;

public class NormalTable implements NormalTableHandler {
    private final GlobalTable table;
    private NormalTableConfig tableConfig;

    public NormalTable(LogicTable logicTable, Partition backendTable, NormalTableConfig tableConfigEntry) {
        this.tableConfig = tableConfigEntry;
        this.table = new GlobalTable(logicTable, Collections.singletonList(backendTable), null);
    }

    @Override
    public Partition getDataNode() {
        return this.table.getGlobalDataNode().get(0);
    }

//    @Override
//    public Function<MySqlInsertStatement, Iterable<TextUpdateInfo>> insertHandler() {
//        return this.table.insertHandler();
//    }

//    @Override
//    public Function<MySqlUpdateStatement, Iterable<TextUpdateInfo>> updateHandler() {
//        return this.table.updateHandler();
//    }

//    @Override
//    public Function<MySqlDeleteStatement, Iterable<TextUpdateInfo>> deleteHandler() {
//        return this.table.deleteHandler();
//    }

    @Override
    public LogicTableType getType() {
        return LogicTableType.NORMAL;
    }

    @Override
    public String getSchemaName() {
        return this.table.getSchemaName();
    }

    @Override
    public String getTableName() {
        return this.table.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return this.table.getCreateTableSQL();
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return this.table.getColumns();
    }

    @Override
    public Map<String, IndexInfo> getIndexes() {
        return table.getIndexes();
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return this.table.getColumnByName(name);
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return this.table.getAutoIncrementColumn();
    }

    @Override
    public String getUniqueName() {
        return this.table.getUniqueName();
    }

    @Override
    public Supplier<Number> nextSequence() {
        return this.table.nextSequence();
    }

    @Override
    public void createPhysicalTables() {
        normalizeCreateTableSQLToMySQL(NormalTable.this.getCreateTableSQL()).ifPresent(sql -> {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            Stream.of(getDataNode())
                    .forEach(node ->
                            createPhysicalTable(jdbcConnectionManager, node, sql));
        });
    }


    @Override
    public void dropPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        Partition dataNode = getDataNode();
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(dataNode.getTargetName())) {
            connection.deleteTable(dataNode.getSchema(),dataNode.getTable());
        }
//        for (DataNode node :Collections.singleton(getDataNode())) {
//            try (DefaultConnection connection = jdbcConnectionManager.getConnection(node.getTargetName())) {
//                connection.executeUpdate(String.format(dropTemplate,node.getSchema(),node.getTable()), false);
//            }
//        }
    }

    public NormalTableConfig getTableConfig() {
        return tableConfig;
    }
}