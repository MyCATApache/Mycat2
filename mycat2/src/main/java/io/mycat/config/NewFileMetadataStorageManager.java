package io.mycat.config;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import java.util.Map;

public interface NewFileMetadataStorageManager {

    public void addSchema(String schemaName, String targetName);

    public void putSchema(LogicSchemaConfig schemaConfig);

    public void dropSchema(String schemaName);

    public void putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString);

    public void putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString, String targetName);


    public void putNormalTable(String schemaName, String tableName, NormalTableConfig normalTableConfig);

    public void putTable(CreateTableConfig createTableConfig);

    public void putGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString);

    public void putGlobalTableConfig(String schemaName, String tableName, GlobalTableConfig globalTableConfig);


    public void removeTable(String schemaNameArg, String tableNameArg);

    public void putRangeTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos);

    public void putShardingTable(String schemaName, String tableName, ShardingTableConfig config);

    public void putHashTable(String schemaName, final String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos);

    public void putUser(String username, String password, String ip, String transactionType);

    public void putUser(UserConfig userConfig);

    public void deleteUser(String username);

    public void putSequence(SequenceConfig sequenceConfig);

    public void removeSequenceByName(String name);

    public void putDatasource(DatasourceConfig datasourceConfig);

    public void removeDatasource(String datasourceName);

    public void putReplica(ClusterConfig clusterConfig);

    public void removeReplica(String replicaName);

    public void putSqlCache(SqlCacheConfig currentSqlCacheConfig);

    public void removeSqlCache(String cacheName);

    public void commit() throws Exception;

    public void close();

    public void putHashTable(String schemaName, String tableName, MySqlCreateTableStatement createTableSql);

    public void reset();
}
