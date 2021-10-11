package io.mycat.config;

import java.util.List;
import java.util.Map;

public interface BaseMetadataStorageManager {

    public void putTable(CreateTableConfig createTableConfig);

    public void removeTable(String schemaNameArg, String tableNameArg);

    public void putSchema(LogicSchemaConfig schemaConfig);

    public void dropSchema(String schemaName);

    public void putUser(UserConfig userConfig);

    public void deleteUser(String username);

    public void putSequence(SequenceConfig sequenceConfig);

    public void removeSequenceByName(String name);

    public void putDatasource(DatasourceConfig datasourceConfig);

    public void removeDatasource(String datasourceName);

    public void putReplica(ClusterConfig clusterConfig);

    public void removeReplica(String replicaName);

    public void reset();

    public void sync(MycatRouterConfig mycatRouterConfig);

    public void putSqlCache(SqlCacheConfig sqlCacheConfig);

    public void removeSqlCache(String name);

    public MycatRouterConfig getConfig();

    public void reportReplica(Map<String, List<String>> dsNames);


}
