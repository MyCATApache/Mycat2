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
package io.mycat.statistic;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.builder.SQLBuilderFactory;
import com.alibaba.druid.sql.builder.SQLSelectBuilder;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.Partition;
import io.mycat.MetaClusterCurrent;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.table.NormalTable;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.TableHandler;
import io.mycat.replica.ReplicaSelectorManager;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticCenter {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCenter.class);
    private final ConcurrentHashMap<Key, StatisticObject> statisticMap = new ConcurrentHashMap<>();
    private final String targetName = "prototype";
    private boolean init = false;

    public StatisticCenter() {
    }

    @SneakyThrows
    public void init() {
//        if (init) {
//            return;
//        }
//        init = true;
//        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//        try (DefaultConnection prototype = jdbcConnectionManager.getConnection(targetName)) {
//            Connection rawConnection = prototype.getRawConnection();
//            JdbcUtils.execute(rawConnection, "CREATE TABLE IF NOT EXISTS mycat.`analyze_table` (\n" +
//                    "  `table_rows` bigint(20) NOT NULL,\n" +
//                    "  `name` varchar(64) NOT NULL\n" +
//                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
//            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select table_rows as `table_rows`,name from mycat.`analyze_table`", Collections.emptyList());
//            for (Map<String, Object> map : maps) {
//                Number table_rows = (Number) map.get("table_rows");
//                String name = (String) map.get("name");
//                String[] strings = name.split("_");
//                StatisticObject statisticObject = new StatisticObject();
//                statisticObject.setRowCount(table_rows.doubleValue());
//                statisticMap.put(Key.of(strings[0], strings[1]), statisticObject);
//            }
//
//        }
    }

    public Double getLogicTableRow(String schemaName, String tableName) {
        StatisticObject statisticObject = statisticMap.get(Key.of(schemaName, tableName));
        if (statisticObject != null) {
            return statisticObject.getRowCount();
        }
        return Double.valueOf(5000000);
    }

    public Double getPhysicsTableRow(String targetName, String schemaName, String tableName) {
        StatisticObject statisticObject = statisticMap.get(Key.of(schemaName, tableName, targetName));
        if (statisticObject != null) {
            return statisticObject.getRowCount();
        }
        return null;
    }

    public void fetchTableRowCount(TableHandler tableHandler) {
        Double aDouble = computeTableRowCount(tableHandler);
        if (aDouble != null) {
            updateRowCount(Key.of(tableHandler.getSchemaName(), tableHandler.getTableName()), aDouble);
        }
    }

    public Double computeTableRowCount(TableHandler tableHandler) {
        try {
            if (tableHandler instanceof GlobalTable) {
                GlobalTable globalTable = (GlobalTable) tableHandler;
                return computeGlobalRowCount(globalTable);
            } else if (tableHandler instanceof ShardingTable) {
                ShardingTable shardingTable = (ShardingTable) tableHandler;
                return computeShardingTableRowCount(shardingTable);
            } else if (tableHandler instanceof NormalTable) {
                NormalTable normalTable = (NormalTable) tableHandler;
                return computeNormalTableRowCount(normalTable);
            }
        } catch (Throwable e) {
            LOGGER.error("统计逻辑表行,物理表行失败", e);
        }
        return null;
    }

    private Double computeNormalTableRowCount(NormalTable normalTable) {
        Partition partition = normalTable.getDataNode();
        String targetName = partition.getTargetName();
        String sql = makeCountSql(partition);
        return fetchRowCount(targetName, sql);
    }

    private Double computeShardingTableRowCount(ShardingTable shardingTable) {
        Double sum = 0d;
        for (Partition backendTableInfo : shardingTable.getBackends()) {
            String targetName = backendTableInfo.getTargetName();
            String sql = makeCountSql(backendTableInfo);
            Double onePhyRowCount = fetchRowCount(targetName, sql);
            if (onePhyRowCount == null) {

            } else {
                sum += onePhyRowCount;
            }
        }
        return sum;
    }

    private Double computeGlobalRowCount(GlobalTable globalTable) {
        Partition backendTableInfo = globalTable.getGlobalDataNode().iterator().next();
        String targetName = backendTableInfo.getTargetName();
        String sql = makeCountSql(backendTableInfo);
        return fetchRowCount(targetName, sql);
    }

    @SneakyThrows
    private void updateRowCount(Key key1, Double value) {
        if (value == null) return;

        //lock
        StatisticObject res = statisticMap.compute(key1, (key, statisticObject) -> {
            if (statisticObject == null) {
                statisticObject = new StatisticObject();
            }
            statisticObject.setRowCount(value);
            return statisticObject;
        });

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
            Connection rawConnection = connection.getRawConnection();
            JdbcUtils.execute(rawConnection, "insert into mycat.analyze_table (table_rows,name) values(?,?)",
                    Arrays.asList(value, key1.getSchemaName() + key1.getTableName()));
        }

        LOGGER.info("行统计更新  tableName:" + key1 + " " + res);
    }

    private String makeCountSql(Partition schemaInfo) {
        SQLSelectBuilder selectSQLBuilder = SQLBuilderFactory.createSelectSQLBuilder(DbType.mysql);
        return selectSQLBuilder.from(schemaInfo.getTargetSchemaTable()).select("count(1)").toString();
    }


    private Double fetchRowCount(String targetName, String sql) {
        try {
            ReplicaSelectorManager runtime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            targetName = runtime.getDatasourceNameByReplicaName(targetName, false, null);
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
                try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql)) {
                    rowBaseIterator.next();
                    return rowBaseIterator.getBigDecimal(1).doubleValue();
                }
            }
        } catch (Throwable e) {
            LOGGER.error("不能获取行统计 " + targetName + " " + sql, e);
            return null;
        }
    }

    @Getter
    @EqualsAndHashCode
    @ToString
    private static class Key {
        final String schemaName;
        final String tableName;
        final String targetName;

        //逻辑表
        public Key(String schemaName, String tableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.targetName = null;
        }

        public Key(String schemaName, String tableName, String targetName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.targetName = targetName;
        }

        public static Key of(String schemaName, String tableName) {
            return new Key(schemaName, tableName);
        }

        public static Key of(String schemaName, String tableName, String targetName) {
            return new Key(schemaName, tableName, targetName);
        }
    }

    @Data
    static class StatisticObject {
        private Double rowCount;
    }


}