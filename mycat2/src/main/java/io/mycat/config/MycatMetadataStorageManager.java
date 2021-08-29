package io.mycat.config;

import io.vertx.core.json.Json;

import java.util.List;
import java.util.Map;

public class MycatMetadataStorageManager implements BaseMetadataStorageManager {
    BaseMetadataStorageManager memory;
    BaseMetadataStorageManager file;
    BaseMetadataStorageManager db;

    public MycatMetadataStorageManager(BaseMetadataStorageManager memory, BaseMetadataStorageManager file, BaseMetadataStorageManager db) {
        this.memory = memory;
        this.file = file;
        this.db = db;
    }


    @Override
    public void putSchema(LogicSchemaConfig schemaConfig) {
        memory.putSchema(schemaConfig);
        file.putSchema(schemaConfig);

        if (db != null) {
            db.putSchema(schemaConfig);
        }
    }


    @Override
    public void dropSchema(String schemaName) {
        memory.dropSchema(schemaName);
        file.dropSchema(schemaName);
        if (db != null) {
            db.dropSchema(schemaName);
        }
    }


    @Override
    public void putTable(CreateTableConfig createTableConfig) {
        memory.putTable(createTableConfig);
        file.putTable(createTableConfig);
        if (db != null) {
            db.putTable(createTableConfig);
        }
    }


    @Override
    public void removeTable(String schemaNameArg, String tableNameArg) {
        memory.removeTable(schemaNameArg, tableNameArg);
        file.removeTable(schemaNameArg, tableNameArg);
        if (db != null) {
            db.removeTable(schemaNameArg, tableNameArg);
        }
    }


    @Override
    public void putUser(UserConfig userConfig) {
        memory.putUser(userConfig);
        file.putUser(userConfig);
        if (db != null) {
            db.putUser(userConfig);
        }
    }

    @Override
    public void deleteUser(String username) {
        memory.deleteUser(username);
        file.deleteUser(username);
        if (db != null) {
            db.deleteUser(username);
        }
    }

    @Override
    public void putSequence(SequenceConfig sequenceConfig) {
        memory.putSequence(sequenceConfig);
        file.putSequence(sequenceConfig);
        if (db != null) {
            db.putSequence(sequenceConfig);
        }
    }

    @Override
    public void removeSequenceByName(String name) {
        memory.removeSequenceByName(name);
        file.removeSequenceByName(name);
        if (db != null) {
            db.removeSequenceByName(name);
        }
    }

    @Override
    public void putDatasource(DatasourceConfig datasourceConfig) {
        memory.putDatasource(datasourceConfig);
        file.putDatasource(datasourceConfig);
        if (db != null) {
            db.putDatasource(datasourceConfig);
        }
    }

    @Override
    public void removeDatasource(String datasourceName) {
        memory.removeDatasource(datasourceName);
        file.removeDatasource(datasourceName);
        if (db != null) {
            db.removeDatasource(datasourceName);
        }
    }

    @Override
    public void putReplica(ClusterConfig clusterConfig) {
        memory.putReplica(clusterConfig);
        file.putReplica(clusterConfig);
        if (db != null) {
            db.putReplica(clusterConfig);
        }
    }

    @Override
    public void removeReplica(String replicaName) {
        memory.removeReplica(replicaName);
        file.removeReplica(replicaName);
        if (db != null) {
            db.removeReplica(replicaName);
        }
    }

    @Override
    public void putSqlCache(SqlCacheConfig currentSqlCacheConfig) {
        memory.putSqlCache(currentSqlCacheConfig);
        file.putSqlCache(currentSqlCacheConfig);
        if (db != null) {
            db.putSqlCache(currentSqlCacheConfig);
        }
    }

    @Override
    public void removeSqlCache(String cacheName) {
        memory.removeSqlCache(cacheName);
        file.removeSqlCache(cacheName);
        if (db != null) {
            db.removeSqlCache(cacheName);
        }
    }

    @Override
    public MycatRouterConfig getConfig() {
        MycatRouterConfig config = memory.getConfig();
//        String memoryConfigText = Json.encodePrettily(config);
//        String fileConfigText = Json.encodePrettily(file.getConfig());
//        if (memoryConfigText.equals(fileConfigText)) {
//            return config;
//        }
//        throw new UnsupportedOperationException();
        return config;
    }


    @Override
    public void reset() {
        memory.reset();
        file.reset();
        if (db != null) {
            db.reset();
        }
    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {
        memory.reportReplica(dsNames);
        file.reportReplica(dsNames);
        if (db != null) {
            db.reportReplica(dsNames);
        }
    }
    public void syncMemoryToAll() {
        MycatRouterConfig mycatRouterConfig = memory.getConfig();
        file.sync(mycatRouterConfig);
        if (db != null) {
            db.sync(mycatRouterConfig);
        }
    }

    @Override
    public void sync(MycatRouterConfig mycatRouterConfig) {
        memory.sync(mycatRouterConfig);
        file.sync(mycatRouterConfig);
        if (db != null) {
            db.sync(mycatRouterConfig);
        }
    }
    public void syncConfigFromFile(){
        MycatRouterConfig config = file.getConfig();
        memory.sync(config);
    }
    public void syncConfigFromFileToDb() {
        MycatRouterConfig config = file.getConfig();
        memory.sync(config);
        if (db != null) {
            db.sync(config);
        }
    }

    public void syncConfigFromDbToFile() {
        if (db != null) {
            MycatRouterConfig config = db.getConfig();
            memory.sync(config);
            file.sync(config);
        }
    }

    public boolean check() {
        MycatRouterConfig config = memory.getConfig();
        MycatRouterConfig config1 = file.getConfig();
        if (config.equals(config1)) {
            if (db != null) {
                if (config.equals(db.getConfig())) {
                    return true;
                }
            }
            return true;
        }
        return false;
    }

}
