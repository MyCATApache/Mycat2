package io.mycat.config;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

import java.util.Map;

public class MetadataStorageManagerHelper implements ConfigOps{
    final BaseMetadataStorageManager metadataStorageManager;

    public MetadataStorageManagerHelper(BaseMetadataStorageManager metadataStorageManager) {
        this.metadataStorageManager = metadataStorageManager;
    }

    @Override
    public void addSchema(String schemaName, String targetName) {

    }

    @Override
    public void putSchema(LogicSchemaConfig schemaConfig) {

    }

    @Override
    public void dropSchema(String schemaName) {

    }

    @Override
    public void putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {

    }

    @Override
    public NormalTableConfig putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString, String targetName) {
        return null;
    }

    @Override
    public NormalTableConfig putNormalTable(String schemaName, String tableName, NormalTableConfig normalTableConfig) {
        return null;
    }

    @Override
    public void putTable(CreateTableConfig createTableConfig) {

    }

    @Override
    public GlobalTableConfig putGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        return null;
    }

    @Override
    public GlobalTableConfig putGlobalTableConfig(String schemaName, String tableName, GlobalTableConfig globalTableConfig) {
        return null;
    }

    @Override
    public void removeTable(String schemaNameArg, String tableNameArg) {

    }

    @Override
    public ShardingTableConfig putRangeTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        return null;
    }

    @Override
    public ShardingTableConfig putShardingTable(String schemaName, String tableName, ShardingTableConfig config) {
        return null;
    }

    @Override
    public ShardingTableConfig putHashTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        return null;
    }

    @Override
    public void putUser(String username, String password, String ip, String transactionType) {

    }

    @Override
    public void putUser(UserConfig userConfig) {

    }

    @Override
    public void deleteUser(String username) {

    }

    @Override
    public void putSequence(SequenceConfig sequenceConfig) {

    }

    @Override
    public void removeSequenceByName(String name) {

    }

    @Override
    public void putDatasource(DatasourceConfig datasourceConfig) {

    }

    @Override
    public void removeDatasource(String datasourceName) {

    }

    @Override
    public void putReplica(ClusterConfig clusterConfig) {

    }

    @Override
    public void removeReplica(String replicaName) {

    }

    @Override
    public void putSqlCache(SqlCacheConfig currentSqlCacheConfig) {

    }

    @Override
    public void removeSqlCache(String cacheName) {

    }

    @Override
    public void commit() throws Exception {

    }

    @Override
    public void close() {

    }

    @Override
    public void reset() {

    }
}
