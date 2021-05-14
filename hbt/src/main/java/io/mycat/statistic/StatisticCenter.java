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

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.*;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.statistic.histogram.MySQLHistogram;
import io.mycat.statistic.histogram.MycatHistogram;
import io.mycat.util.JsonUtil;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

public class StatisticCenter {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCenter.class);
    private final ConcurrentHashMap<Key, StatisticObject> statisticMap = new ConcurrentHashMap<>();
    private final String targetName = "prototype";
    private boolean init = false;
    private final DialectStatistic dialectStatistic = new MySQLDialectStatisticImpl();

    public StatisticCenter() {
    }

    @SneakyThrows
    public void init() {
        if (init) {
            return;
        }
        init = true;
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection prototype = jdbcConnectionManager.getConnection(targetName)) {
            Connection rawConnection = prototype.getRawConnection();
            JdbcUtils.execute(rawConnection, "CREATE TABLE IF NOT EXISTS mycat.`analyze_table` (\n" +
                    "  `table_rows` bigint(20) NOT NULL,\n" +
                    "  `name` varchar(64) NOT NULL\n" +
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(rawConnection, "select table_rows as `table_rows`,name from mycat.`analyze_table`", Collections.emptyList());
            for (Map<String, Object> map : maps) {
                Number table_rows = (Number) map.get("table_rows");
                String name = (String) map.get("name");
                String[] strings = name.split("_");
                StatisticObject statisticObject = new StatisticObject(table_rows.doubleValue(),JsonUtil.from((String) map.get("histogram"),StatisticObject.class).getColumnHistogramMap());
                statisticMap.put(Key.of(strings[0], strings[1]), statisticObject);
            }

        }
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
        StatisticObject statisticObject = computeTableRowCount(tableHandler);
        if (statisticObject != null) {
            updateRowCount(Key.of(tableHandler.getSchemaName(), tableHandler.getTableName()), statisticObject);
        }
    }

    public StatisticObject computeTableRowCount(TableHandler tableHandler) {
        String schemaName = tableHandler.getSchemaName();
        String tableName = tableHandler.getTableName();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        try {
            if (tableHandler instanceof GlobalTable) {
                GlobalTable globalTable = (GlobalTable) tableHandler;
                double rowCount = computeGlobalRowCount(globalTable);
                DataNode backendTableInfo = globalTable.getGlobalDataNode().iterator().next();
                Map<String, MycatHistogram> columnHistograms = new HashMap<>();
                for (SimpleColumnInfo column : globalTable.getColumns()) {
                    String columnName = column.getColumnName();
                    Optional<MySQLHistogram> histogramOptional = dialectStatistic.histogram(backendTableInfo.getTargetName(), backendTableInfo.getSchema(), backendTableInfo.getTable(), columnName);
                    histogramOptional.ifPresent(c -> {
                        columnHistograms.put(columnName, MycatHistogram.of(rowCount, metadataManager, schemaName, tableName, columnName, c));
                    });
                }
                return new StatisticObject(rowCount, columnHistograms);

            } else if (tableHandler instanceof ShardingTable) {
                ShardingTable shardingTable = (ShardingTable) tableHandler;
                double rowCount = computeShardingTableRowCount(shardingTable);

                Map<String, MycatHistogram> columnHistograms = new HashMap<>();

                List<DataNode> dataNodes = shardingTable.getShardingFuntion().calculate(Collections.emptyMap());
                out:
                for (SimpleColumnInfo column : shardingTable.getColumns()) {
                    String columnName = column.getColumnName();

                    List<Optional<MySQLHistogram>> list = new ArrayList<>();
                    for (DataNode backendTableInfo : dataNodes) {
                        Optional<MySQLHistogram> histogramOptional = dialectStatistic.histogram(backendTableInfo.getTargetName(), backendTableInfo.getSchema(), backendTableInfo.getTable(), columnName);
                        if (!histogramOptional.isPresent()) {
                          continue out;
                        }else {
                            list.add(histogramOptional);
                        }
                    }
                    list.stream().map(i->i.get()).map(i->MycatHistogram.of(rowCount,metadataManager,schemaName,tableName,columnName,i))
                    .reduce((mycatHistogram, mycatHistogram2) -> mycatHistogram.merge(mycatHistogram2))
                    .ifPresent(c->columnHistograms.put(columnName,c));

                }
                return new StatisticObject(rowCount, columnHistograms);

            } else if (tableHandler instanceof NormalTable) {
                NormalTable normalTable = (NormalTable) tableHandler;
                DataNode backendTableInfo = normalTable.getDataNode();
                double rowCount = computeNormalTableRowCount(normalTable);
                Map<String, MycatHistogram> columnHistograms = new HashMap<>();

                for (SimpleColumnInfo column : normalTable.getColumns()) {
                    String columnName = column.getColumnName();
                    Optional<MySQLHistogram> histogramOptional = dialectStatistic.histogram(backendTableInfo.getTargetName(), backendTableInfo.getSchema(), backendTableInfo.getTable(), columnName);
                    histogramOptional.ifPresent(c -> {
                        columnHistograms.put(columnName, MycatHistogram.of(rowCount, metadataManager, schemaName, tableName, columnName, c));
                    });
                }
                return new StatisticObject(rowCount, columnHistograms);
            }
        } catch (Throwable e) {
            LOGGER.error("统计逻辑表行,物理表行失败", e);
        }
        return null;
    }

    private Double computeNormalTableRowCount(NormalTable normalTable) {
        DataNode dataNode = normalTable.getDataNode();
        String targetName = dataNode.getTargetName();
        return dialectStatistic.rowCount(targetName, dataNode.getSchema(), dataNode.getTable());
    }

    private Double computeShardingTableRowCount(ShardingTable shardingTable) {
        Double sum = 0d;
        for (DataNode backendTableInfo : shardingTable.getBackends()) {
            Double onePhyRowCount = dialectStatistic.rowCount(backendTableInfo.getTargetName(), backendTableInfo.getSchema(), backendTableInfo.getTable());
            if (onePhyRowCount == null) {

            } else {
                sum += onePhyRowCount;
            }
        }
        return sum;
    }

    private Double computeGlobalRowCount(GlobalTable globalTable) {
        DataNode backendTableInfo = globalTable.getGlobalDataNode().iterator().next();
        return dialectStatistic.rowCount(backendTableInfo.getTargetName(), backendTableInfo.getSchema(), backendTableInfo.getTable());
    }

    @SneakyThrows
    private void updateRowCount(Key key1, StatisticObject statisticObject) {
        if (statisticObject == null) return;

        //lock
        statisticMap.put(key1,statisticObject);

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(targetName)) {
            Connection rawConnection = connection.getRawConnection();
            JdbcUtils.execute(rawConnection, "insert into mycat.analyze_table (table_rows,histogram,name) values(?,?,?)",
                    Arrays.asList(statisticObject.getRowCount(), JsonUtil.toJson(statisticObject), key1.getSchemaName() + key1.getTableName()));
        }

        LOGGER.info("行统计更新  tableName:" + key1 + " " + statisticObject);
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
    @AllArgsConstructor
    public static class StatisticObject {
        private final Double rowCount;
        private final Map<String, MycatHistogram> columnHistogramMap;
    }


}