package io.mycat.statistic;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.builder.SQLBuilderFactory;
import com.alibaba.fastsql.sql.builder.SQLSelectBuilder;
import io.mycat.BackendTableInfo;
import io.mycat.DataNode;
import io.mycat.SchemaInfo;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.metadata.GlobalTable;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.ShardingTable;
import io.mycat.TableHandler;
import io.mycat.replica.ReplicaSelectorRuntime;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public enum StatisticCenter {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCenter.class);
    MetadataManager metadataManager;
    final ConcurrentHashMap<Key, StatisticObject> statisticMap = new ConcurrentHashMap<>();


    public Double getLogicTableRow(String schemaName, String tableName) {
        StatisticObject statisticObject = statisticMap.get(Key.of(schemaName, tableName));
        if (statisticObject != null) {
            return statisticObject.getRowCount();
        }
        return null;
    }

    public Double getPhysicsTableRow(String targetName, String schemaName, String tableName) {
        StatisticObject statisticObject = statisticMap.get(Key.of(schemaName, tableName, targetName));
        if (statisticObject != null) {
            return statisticObject.getRowCount();
        }
        return null;
    }


    public void computeTableRowCount() {
        Optional.ofNullable(metadataManager)
                .map(i -> i.getSchemaMap())
                .ifPresent(schemarMap -> schemarMap.entrySet().stream()
                        .flatMap(i -> i.getValue().logicTables().entrySet().stream())
                        .map(i -> i.getValue())
                        .forEach(tableHandler -> {
                            computeTableRowCount(tableHandler);
                        }));
    }

    public void computeTableRowCount(TableHandler tableHandler) {
        try {
            if (tableHandler instanceof GlobalTable) {
                GlobalTable globalTable = (GlobalTable) tableHandler;
                computeGlobalRowCount(globalTable);
            } else if (tableHandler instanceof ShardingTable) {
                ShardingTable shardingTable = (ShardingTable) tableHandler;
                computeShardingTableRowCount(shardingTable);
            }
        } catch (Throwable e) {
            LOGGER.error("统计逻辑表行,物理表行失败", e);
        }
    }

    public void computeShardingTableRowCount(ShardingTable shardingTable) {
        Double sum = 0d;
        for (DataNode backendTableInfo : shardingTable.getBackends()) {

            String targetName = backendTableInfo.getTargetName();
            String sql = makeCountSql(backendTableInfo);
            Double onePhyRowCount = fetchRowCount(targetName, sql);
            if (onePhyRowCount == null) {
                return;//退出
            } else {
                sum += onePhyRowCount;
                //物理表
                Key key = Key.of(backendTableInfo.getTargetName(),
                        backendTableInfo.getTable(),
                        backendTableInfo.getTargetName());
                updateRowCount(key, sum);
            }
            //逻辑表
            Key key = Key.of(shardingTable.getSchemaName(), shardingTable.getTableName());
            updateRowCount(key, sum);
        }
    }

    private void computeGlobalRowCount(GlobalTable globalTable) {
        DataNode backendTableInfo = globalTable.getGlobalDataNode().iterator().next();


        String targetName = backendTableInfo.getTargetName();
        String sql = makeCountSql(backendTableInfo);


        Double value = fetchRowCount(targetName, sql);

        if (value != null) {
            //逻辑表
            Key logicKey = Key.of(globalTable.getSchemaName(), globalTable.getTableName());
            updateRowCount(logicKey, value);

            //物理表
            globalTable.getGlobalDataNode().stream().map(tableInfo -> {
                return Key.of(tableInfo.getSchema(), tableInfo.getTable(), tableInfo.getTargetName());
            }).forEach(key -> {
                updateRowCount(key, value);
            });
        }
    }

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

        LOGGER.info("行统计更新  tableName:" + key1 + " " + res);
    }

    private String makeCountSql(DataNode schemaInfo) {
        SQLSelectBuilder selectSQLBuilder = SQLBuilderFactory.createSelectSQLBuilder(DbType.mysql);
        return selectSQLBuilder.from(schemaInfo.getTargetSchemaTable()).select("count(*)").toString();
    }


    private Double fetchRowCount(String targetName, String sql) {
        try {
            targetName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(targetName, false, null);
            try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(targetName)) {
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