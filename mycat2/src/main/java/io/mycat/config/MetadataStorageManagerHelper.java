package io.mycat.config;

import com.alibaba.druid.sql.MycatSQLUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.util.NameMap;

import java.util.*;
import java.util.stream.Collectors;

public class MetadataStorageManagerHelper implements BaseMetadataStorageManager {
    final BaseMetadataStorageManager metadataStorageManager;

    public MetadataStorageManagerHelper(BaseMetadataStorageManager metadataStorageManager) {
        this.metadataStorageManager = metadataStorageManager;
    }

    public void addSchema(String schemaName, String targetName) {
        LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
        logicSchemaConfig.setSchemaName(schemaName);
        logicSchemaConfig.setTargetName(targetName);
        putSchema(logicSchemaConfig);
    }


    public void putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        SchemaHandler schemaHandler = metadataManager.getSchemaMap().get(schemaName);
        String defaultTarget = Optional.ofNullable(schemaHandler.defaultTargetName()).orElse(metadataManager.getPrototype());
        putNormalTable(schemaName, tableName, sqlString, defaultTarget);
    }

    public NormalTableConfig putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString, String targetName) {

        NormalTableConfig normalTableConfig = new NormalTableConfig();
        normalTableConfig.setCreateTableSQL(sqlString.toString());
        normalTableConfig.setLocality(NormalBackEndTableInfoConfig.builder()
                .targetName(targetName)
                .schemaName(schemaName)
                .tableName(tableName)
                .build());

        return putNormalTable(schemaName, tableName, normalTableConfig);
    }

    public NormalTableConfig putNormalTable(String schemaName, String tableName, NormalTableConfig normalTableConfig) {
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setSchemaName(schemaName);
        createTableConfig.setTableName(tableName);
        createTableConfig.setNormalTable(normalTableConfig);
        metadataStorageManager.putTable(createTableConfig);
        return normalTableConfig;
    }

    public GlobalTableConfig putGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        GlobalTableConfig globalTableConfig = getGlobalTableConfig(sqlString);
        return putGlobalTable(schemaName, tableName, globalTableConfig);
    }

    public GlobalTableConfig putGlobalTable(String schemaName, String tableName, GlobalTableConfig globalTableConfig) {
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setSchemaName(schemaName);
        createTableConfig.setTableName(tableName);
        createTableConfig.setGlobalTable(globalTableConfig);
        metadataStorageManager.putTable(createTableConfig);
        return globalTableConfig;
    }

    private GlobalTableConfig getGlobalTableConfig(MySqlCreateTableStatement sqlString) {
        ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);

        List<String> allReplica = replicaSelectorManager.getReplicaMap().keySet().stream().filter(i -> i.startsWith("c")).collect(Collectors.toList());
        GlobalTableConfig globalTableConfig = new GlobalTableConfig();
        globalTableConfig.setCreateTableSQL(sqlString.toString());
        globalTableConfig.setBroadcast(allReplica.stream()
                .map(i -> {
                    GlobalBackEndTableInfoConfig backEndTableInfoConfig = new GlobalBackEndTableInfoConfig();
                    backEndTableInfoConfig.setTargetName(i);
                    return backEndTableInfoConfig;
                }).collect(Collectors.toList()));
        return globalTableConfig;
    }

    public ShardingTableConfig putRangeTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        Map<String, String> ranges = (Map) infos.get("ranges");
        Map<String, String> dataNodes = (Map) infos.get("dataNodes");
        Map<String, String> properties = (Map) infos.get("properties");
        String aClass = (String) (infos.get("class"));
        String name = (String) (infos.get("name"));
        ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
        ShardingTableConfig config = builder
                .createTableSQL(tableStatement.toString())
                .function(ShardingFunction.builder().name(name).clazz(aClass).properties((Map) properties).ranges((Map) ranges).build())
                .partition(Optional.ofNullable(dataNodes).map(i -> ShardingBackEndTableInfoConfig
                        .builder()
                        .schemaNames(dataNodes.get("schemaNames"))
                        .tableNames(dataNodes.get("tableNames"))
                        .targetNames(dataNodes.get("targetNames")).build())
                        .orElse(null))
                .build();

        return putShardingTable(schemaName, tableName, config);
    }

    public ShardingTableConfig putShardingTable(String schemaName, String tableName, ShardingTableConfig config) {
        CreateTableConfig createTableConfig = new CreateTableConfig();
        createTableConfig.setSchemaName(schemaName);
        createTableConfig.setTableName(tableName);
        createTableConfig.setShardingTable(config);
        metadataStorageManager.putTable(createTableConfig);
        return config;
    }
    public void putHashTable(String schemaName, String tableName, MySqlCreateTableStatement createTableSql) {
        putHashTable(schemaName, tableName, createTableSql, getAutoHashProperties(createTableSql));
    }


    public ShardingTableConfig putHashTable(String schemaName,final String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        NameMap<SQLColumnDefinition> columnMap = NameMap.immutableCopyOf(tableStatement.getColumnDefinitions().stream()
                .collect(Collectors.toMap(k -> SQLUtils.normalize(k.getColumnName()), v -> v)));

        Map<String, ShardingTableConfig> indexTableConfigs = new HashMap<>();
        MySqlPrimaryKey primaryKey =(MySqlPrimaryKey) tableStatement.getTableElementList().stream().filter(i -> i instanceof MySqlPrimaryKey).findFirst().orElse(null);
        for (SQLTableElement sqlTableElement : tableStatement.getTableElementList()) {
            if (sqlTableElement instanceof MySqlTableIndex) {
                MySqlTableIndex element = (MySqlTableIndex) sqlTableElement;
                if(!element.isGlobal()){
                    continue;
                }
                SQLIndexDefinition indexDefinition = element.getIndexDefinition();
                MySqlCreateTableStatement indexCreateTableStatement = new MySqlCreateTableStatement();
                indexCreateTableStatement.setIfNotExiists(true);

                String indexTableName = tableName + "_" + SQLUtils.normalize(indexDefinition.getName().getSimpleName());
                indexCreateTableStatement.setTableName(indexTableName);
                indexCreateTableStatement.setSchema(schemaName);
                for (SQLSelectOrderByItem indexColumn : indexDefinition.getColumns()) {
                    indexCreateTableStatement.addColumn(columnMap.get(SQLUtils.normalize(indexColumn.getExpr().toString())));
                }
                for (SQLName sqlName : indexDefinition.getCovering()) {
                    indexCreateTableStatement.addColumn(columnMap.get(SQLUtils.normalize(sqlName.toString())));
                }
                if(primaryKey!=null){
                    indexCreateTableStatement.getTableElementList().add(primaryKey);
                }
                indexCreateTableStatement.setDbPartitionBy(indexDefinition.getDbPartitionBy());
                indexCreateTableStatement.setTablePartitionBy(indexDefinition.getTbPartitionBy());

                indexCreateTableStatement.setDbPartitions(indexCreateTableStatement.getDbPartitions());
                indexCreateTableStatement.setTablePartitions(indexDefinition.getTbPartitions());
                Map<String, Object> autoHashProperties = getAutoHashProperties(indexCreateTableStatement);

                ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
                ShardingTableConfig config = builder
                        .createTableSQL(MycatSQLUtils.toString(indexCreateTableStatement))
                        .function(ShardingFunction.builder().properties(autoHashProperties).build())
                        .build();

                indexTableConfigs.put(indexTableName, config);
            }
        }

        ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
        ShardingTableConfig config = builder
                .createTableSQL(MycatSQLUtils.toString(tableStatement))
                .function(ShardingFunction.builder().properties((Map) infos).build())
                .shardingIndexTables(indexTableConfigs)
                .build();
        return putShardingTable(schemaName, tableName, config);
    }

    public static Map<String, Object> getAutoHashProperties(MySqlCreateTableStatement createTableSql) {
        SQLExpr dbPartitionBy = createTableSql.getDbPartitionBy();
        HashMap<String, Object> properties = new HashMap<>();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        int defaultStoreNodeNum = metadataManager.getDefaultStoreNodeNum();
        properties.put("storeNum", defaultStoreNodeNum);
        if (dbPartitionBy != null) {
            int dbPartitions = (Optional.ofNullable(createTableSql.getDbPartitions())
                    .map(i -> i.toString()).map(i -> Integer.parseInt(SQLUtils.normalize(i))).orElse(defaultStoreNodeNum));
            properties.put("dbNum", Objects.toString(dbPartitions));
            properties.put("dbMethod", Objects.toString(dbPartitionBy));
        }

        SQLExpr tablePartitionBy = createTableSql.getTablePartitionBy();
        if (tablePartitionBy != null) {
            int tablePartitions = Integer.parseInt(SQLUtils.normalize(createTableSql.getTablePartitions().toString()));
            properties.put("tableNum", Objects.toString(tablePartitions));
            properties.put("tableMethod", Objects.toString(tablePartitionBy));
        }
        return properties;
    }



    ///////////////////////////////////////common///////////////////////////////////////


    ////////////////////////////////////////////////////////////////////////////////////


    @Override
    public void putTable(CreateTableConfig createTableConfig) {
        metadataStorageManager.putTable(createTableConfig);
    }

    @Override
    public void removeTable(String schemaNameArg, String tableNameArg) {
        metadataStorageManager.removeTable(schemaNameArg, tableNameArg);
    }

    @Override
    public void putSchema(LogicSchemaConfig schemaConfig) {
        metadataStorageManager.putSchema(schemaConfig);
    }

    @Override
    public void dropSchema(String schemaName) {
        metadataStorageManager.dropSchema(schemaName);
    }

    @Override
    public void putUser(UserConfig userConfig) {
        metadataStorageManager.putUser(userConfig);
    }

    @Override
    public void deleteUser(String username) {
        metadataStorageManager.deleteUser(username);
    }

    @Override
    public void putSequence(SequenceConfig sequenceConfig) {
        metadataStorageManager.putSequence(sequenceConfig);
    }

    @Override
    public void removeSequenceByName(String name) {
        metadataStorageManager.removeSequenceByName(name);
    }

    @Override
    public void putDatasource(DatasourceConfig datasourceConfig) {
        metadataStorageManager.putDatasource(datasourceConfig);
    }

    @Override
    public void removeDatasource(String datasourceName) {
        metadataStorageManager.removeDatasource(datasourceName);
    }

    @Override
    public void putReplica(ClusterConfig clusterConfig) {
        metadataStorageManager.putReplica(clusterConfig);
    }

    @Override
    public void removeReplica(String replicaName) {
        metadataStorageManager.removeReplica(replicaName);
    }

    @Override
    public void reset() {
        metadataStorageManager.reset();
    }

    @Override
    public void sync(MycatRouterConfig mycatRouterConfig) {
        metadataStorageManager.sync(mycatRouterConfig);
    }

    @Override
    public void putSqlCache(SqlCacheConfig sqlCacheConfig) {
        metadataStorageManager.putSqlCache(sqlCacheConfig);
    }

    @Override
    public void removeSqlCache(String name) {
        metadataStorageManager.removeSqlCache(name);
    }

    @Override
    public MycatRouterConfig getConfig() {
        return null;
    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {

    }
}
