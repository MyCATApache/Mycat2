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
package io.mycat.sqlhandler;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.*;
import io.mycat.sqlhandler.config.FileStorageManagerImpl;
import io.mycat.sqlhandler.config.StdStorageManagerImpl;
import io.mycat.sqlhandler.config.StorageManager;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.List;

public class ConfigUpdater {
    public static MycatRouterConfigOps getOps() {
        return getOps(Options.builder().persistence(true).build());
    }

    public static MycatRouterConfigOps getOps(Options options) {
        StorageManager metadataManager = MetaClusterCurrent.wrapper(StorageManager.class);
        MycatRouterConfig routerConfig = getRuntimeConfigSnapshot();
        return new MycatRouterConfigOps(routerConfig, metadataManager, options);
    }

    @SneakyThrows
    public static void loadConfigFromFile() {
        load(getFileConfigSnapshot());
    }

    @SneakyThrows
    public static void writeDefaultConfigToFile() {
        MycatRouterConfigOps mycatRouterConfigOps = getOps();
        mycatRouterConfigOps.reset();
        mycatRouterConfigOps.commit();
    }

    @SneakyThrows
    public static void load(MycatRouterConfig mycatRouterConfig) {
        StorageManager storageManager = MetaClusterCurrent.wrapper(StorageManager.class);
        MycatRouterConfig orginal = MetaClusterCurrent.exist(MycatRouterConfig.class) ?
                MetaClusterCurrent.wrapper(MycatRouterConfig.class) : new MycatRouterConfig();
        MycatRouterConfigOps mycatRouterConfigOps = new MycatRouterConfigOps(orginal,
                storageManager,
                Options.builder()
                        .persistence(false).build(), mycatRouterConfig);
        mycatRouterConfigOps.commit();
    }

    public static StorageManager createStorageManager(Path basePath) {
        FileStorageManagerImpl fileStorageManager = new FileStorageManagerImpl(basePath);
        return new StdStorageManagerImpl(fileStorageManager);
    }


    @NotNull
    public static MycatRouterConfig getRuntimeConfigSnapshot() {
        return MetaClusterCurrent.exist(MycatRouterConfig.class)? MetaClusterCurrent.wrapper(MycatRouterConfig.class):new MycatRouterConfig();
    }


    @NotNull
    public static MycatRouterConfig getFileConfigSnapshot() {
        MycatRouterConfig mycatRouterConfig = new MycatRouterConfig();
        StorageManager metadataManager = MetaClusterCurrent.wrapper(StorageManager.class);
        List<LogicSchemaConfig> logicSchemaConfigs = metadataManager.get(LogicSchemaConfig.class).values();
        List<ClusterConfig> clusterConfigs = metadataManager.get(ClusterConfig.class).values();
        List<DatasourceConfig> datasourceConfigs = metadataManager.get(DatasourceConfig.class).values();
        List<UserConfig> userConfigs = metadataManager.get(UserConfig.class).values();
        List<SequenceConfig> sequenceConfigs = metadataManager.get(SequenceConfig.class).values();
        List<SqlCacheConfig> sqlCacheConfigs = metadataManager.get(SqlCacheConfig.class).values();

        mycatRouterConfig.setSchemas(logicSchemaConfigs);
        mycatRouterConfig.setClusters(clusterConfigs);
        mycatRouterConfig.setDatasources(datasourceConfigs);
        mycatRouterConfig.setUsers(userConfigs);
        mycatRouterConfig.setSequences(sequenceConfigs);
        mycatRouterConfig.setSqlCacheConfigs(sqlCacheConfigs);
        return mycatRouterConfig;
    }
//
//    public static class UserUpdater {
//
//        public static void update(Map<String, UserConfig> config) {
//            MetaClusterCurrent.register(Authenticator.class, new AuthenticatorImpl(config));
//        }
//
//        public static void add(UserConfig config) {
//            Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
//            HashMap<String, UserConfig> map = new HashMap<>(authenticator.getConfigAsMap());
//            map.put(config.getUsername(), config);
//            MetaClusterCurrent.register(Authenticator.class, new AuthenticatorImpl(map));
//        }
//
//        public static void remove(String name) {
//            Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
//            HashMap<String, UserConfig> map = new HashMap<>(authenticator.getConfigAsMap());
//            map.remove(name);
//            MetaClusterCurrent.register(Authenticator.class, new AuthenticatorImpl(map));
//        }
//    }
//
//    public static class SqlCacheUpdater {
//
//        public static void update(List<SqlCacheConfig> sqlCacheConfigList){
//            if(MetaClusterCurrent.exist(SqlResultSetService.class)){
//                MetaClusterCurrent.wrapper(SqlResultSetService.class).clear();
//            }
//            SqlResultSetService sqlResultSetService = new SqlResultSetService();
//            for (SqlCacheConfig sqlCacheConfig : sqlCacheConfigList) {
//                sqlResultSetService.addIfNotPresent(sqlCacheConfig);
//            }
//            MetaClusterCurrent.register(SqlResultSetService.class,sqlResultSetService);
//        }
//
//        public static void add(SqlCacheConfig config){
//            SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
//            sqlResultSetService.addIfNotPresent(config);
//        }
//        public static void remove(String name){
//            SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
//            sqlResultSetService.dropByName(name);
//        }
//    }
//
//    public static class SequenceUpdater {
//
//        public static void update(List<SequenceConfig> sequenceConfigs) {
//            ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
//            SequenceGenerator sequenceGenerator = new SequenceGenerator(serverConfig.getMycatId(), sequenceConfigs);
//            MetaClusterCurrent.register(SequenceGenerator.class, sequenceGenerator);
//        }
//
//        public static void add(SequenceConfig sequenceConfig) {
//            List<SequenceConfig> list = (List) ImmutableList.builder().addAll(MetaClusterCurrent.wrapper(SequenceGenerator.class).getConfigAsList()).add(sequenceConfig).build();
//            update(list);
//        }
//
//        public static void remove(String sequenceConfig) {
//            List<SequenceConfig> list = new ArrayList<>(MetaClusterCurrent.wrapper(SequenceGenerator.class).getConfigAsList());
//            list.removeIf(s->s.getName().equalsIgnoreCase(sequenceConfig));
//            update(list);
//        }
//    }
//
//    public static class JdbcManagerController {
//        public static void update(Map<String, DatasourceConfig> datasourceConfigMap) {
//            if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
//                JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//                jdbcConnectionManager.close();
//            }
//            JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(
//                    DruidDatasourceProvider.class.getName(),
//                    datasourceConfigMap);
//            MetaClusterCurrent.register(JdbcConnectionManager.class, jdbcConnectionManager);
//            jdbcConnectionManager.registerReplicaSelector(MetaClusterCurrent.wrapper(ReplicaSelectorManager.class));
//            DatasourceConfigProvider datasourceConfigProvider = new DatasourceConfigProvider() {
//                @Override
//                public Map<String, DatasourceConfig> get() {
//                    return datasourceConfigMap;
//                }
//            };
//            MetaClusterCurrent.register(DatasourceConfigProvider.class, datasourceConfigProvider);
//        }
//
//        public static void addDatasource(DatasourceConfig config) {
//            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//            jdbcConnectionManager.addDatasource(config);
//            jdbcConnectionManager.registerReplicaSelector(MetaClusterCurrent.wrapper(ReplicaSelectorManager.class));
//            Map<String, DatasourceConfig> configMap = jdbcConnectionManager.getConfig();
//            DatasourceConfigProvider datasourceConfigProvider = new DatasourceConfigProvider() {
//                @Override
//                public Map<String, DatasourceConfig> get() {
//                    return configMap;
//                }
//            };
//            MetaClusterCurrent.register(DatasourceConfigProvider.class, datasourceConfigProvider);
//        }
//
//        public static void removeDatasource(String name) {
//            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
//            jdbcConnectionManager.removeDatasource(name);
//            jdbcConnectionManager.registerReplicaSelector(MetaClusterCurrent.wrapper(ReplicaSelectorManager.class));
//            Map<String, DatasourceConfig> configMap = jdbcConnectionManager.getConfig();
//            DatasourceConfigProvider datasourceConfigProvider = new DatasourceConfigProvider() {
//                @Override
//                public Map<String, DatasourceConfig> get() {
//                    return configMap;
//                }
//            };
//            MetaClusterCurrent.register(DatasourceConfigProvider.class, datasourceConfigProvider);
//        }
//    }
//
//    public static class SchemaController {
//        public static void update(Map<String, LogicSchemaConfig> schemaConfigs){
//            clearCache();
//            MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, "prototype", MetaClusterCurrent.wrapper(JdbcConnectionManager.class));
//            MetaClusterCurrent.register(MetadataManager.class,metadataManager);
//            MetaClusterCurrent.register(DrdsSqlCompiler.class, new DrdsSqlCompiler(new DrdsConfig() {
//                @Override
//                public NameMap<SchemaHandler> schemas() {
//                    return metadataManager.getSchemaMap();
//                }
//            }));
//        }
//
//        private static void clearCache() {
//            MetaClusterCurrent.register(UpdatePlanCache.class, new UpdatePlanCache());
//            if (!MetaClusterCurrent.exist(SqlResultSetService.class)) {
//                MetaClusterCurrent.register(SqlResultSetService.class, new SqlResultSetService());
//            }else {
//                MetaClusterCurrent.wrapper(SqlResultSetService.class).clear();
//            }
//
//            if (!MetaClusterCurrent.exist(MemPlanCache.class)) {
//                DbPlanManagerPersistorImpl newDbPlanManagerPersistor = new DbPlanManagerPersistorImpl();
//                MemPlanCache memPlanCache = new MemPlanCache((newDbPlanManagerPersistor));
//                MetaClusterCurrent.register(MemPlanCache.class, memPlanCache);
//                MetaClusterCurrent.register(QueryPlanner.class, new QueryPlanner(memPlanCache));
//            } else {
//                MemPlanCache memPlanCache = MetaClusterCurrent.wrapper(MemPlanCache.class);
//                memPlanCache.clearCache();
//            }
//        }
//
//        public static void addTable(CreateTableConfig createTableConfig){
//            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
//            String schemaName = createTableConfig.getSchemaName();
//            String tableName = createTableConfig.getTableName();
//            removeTable(schemaName, tableName);
//            GlobalTableConfig globalTable = createTableConfig.getGlobalTable();
//            if (globalTable != null) {
//                metadataManager.addGlobalTable(schemaName, tableName, globalTable);
//            }
//            NormalTableConfig normalTable = createTableConfig.getNormalTable();
//            if (normalTable != null) {
//                metadataManager.addNormalTable(schemaName, tableName, normalTable);
//            }
//            ShardingTableConfig shardingTable = createTableConfig.getShardingTable();
//            if (shardingTable != null) {
//                metadataManager.addShardingTable(schemaName, tableName, shardingTable);
//            }
//            TableHandler table = metadataManager.getTable(schemaName, tableName);
//            table.createPhysicalTables();
//        }
//        public static void removeTable(String schema,String table){
//            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
//            metadataManager.removeTable(schema, table);
//            clearCache();
//        }
//        public static void addSchema(LogicSchemaConfig schemaConfig){
//            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
//            metadataManager.addSchema(schemaConfig);
//        }
//        public static void removeSchema(String schema){
//            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
//            MetaClusterCurrent.register(UpdatePlanCache.class, new UpdatePlanCache());
//            metadataManager.removeSchema(schema);
//            MemPlanCache memPlanCache = MetaClusterCurrent.wrapper(MemPlanCache.class);
//            memPlanCache.clearCache();
//        }
//
//
//    }
//    public static class ClusterController {
//        public static void update(List<ClusterConfig> clusterConfigs, Map<String, DatasourceConfig> datasourceConfigMap) {
//            if (MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
//                MetaClusterCurrent.wrapper(ReplicaSelectorManager.class).close();
//            }
//            ReplicaSelectorManager replicaSelector = ReplicaSelectorRuntime.create(clusterConfigs, MetaClusterCurrent.wrapper(LoadBalanceManager.class), datasourceConfigMap);
//            MetaClusterCurrent.register(ReplicaSelectorManager.class, replicaSelector);
//            if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
//                MetaClusterCurrent.wrapper(JdbcConnectionManager.class).registerReplicaSelector(replicaSelector);
//            }
//        }
//
//        public static void addCluster(ClusterConfig clusterConfig){
//            ReplicaSelectorManager originalReplicaSelector = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
//            List<ClusterConfig> config = new ArrayList<>(originalReplicaSelector.getConfig());
//            config.removeIf(c -> c.getName().equalsIgnoreCase(clusterConfig.getName()));
//            config.add(clusterConfig);
//            update(config, MetaClusterCurrent.wrapper(JdbcConnectionManager.class).getConfig());
//        }
//        public static void removeCluster(String name){
//            ReplicaSelectorManager originalReplicaSelector = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
//            List<ClusterConfig> config = new ArrayList<>(originalReplicaSelector.getConfig());
//            config.removeIf(c -> c.getName().equalsIgnoreCase(name));
//            update(config, MetaClusterCurrent.wrapper(JdbcConnectionManager.class).getConfig());
//        }
//    }
//
//
//


}
