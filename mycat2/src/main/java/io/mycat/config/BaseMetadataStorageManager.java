package io.mycat.config;

import com.google.common.collect.Lists;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.MysqlMetadataManager;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.replica.ReplicaType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    default public void reset() {
        MycatRouterConfig mycatRouterConfig = new MycatRouterConfig();
        defaultConfig(mycatRouterConfig);
        sync(mycatRouterConfig);
    }

    public void sync(MycatRouterConfig mycatRouterConfig);

    public void putSqlCache(SqlCacheConfig sqlCacheConfig);
    public void removeSqlCache(String name);

    public static void defaultConfig(MycatRouterConfig routerConfig) {

        routerConfig.setSchemas(new ArrayList<>(MysqlMetadataManager.getMySQLSystemTables().values()));

        if (routerConfig.getUsers().isEmpty()) {
            UserConfig userConfig = new UserConfig();
            userConfig.setPassword("123456");
            userConfig.setUsername("root");
            routerConfig.getUsers().add(userConfig);
        }

        if (routerConfig.getDatasources().isEmpty()) {
            DatasourceConfig datasourceConfig = new DatasourceConfig();
            datasourceConfig.setDbType("mysql");
            datasourceConfig.setUser("root");
            datasourceConfig.setPassword("123456");
            datasourceConfig.setName("prototypeDs");
            datasourceConfig.setUrl("jdbc:mysql://localhost:3306/mysql");
            routerConfig.getDatasources().add(datasourceConfig);

            if (routerConfig.getClusters().isEmpty()) {
                ClusterConfig clusterConfig = new ClusterConfig();
                clusterConfig.setName("prototype");
                clusterConfig.setMasters(Lists.newArrayList("prototypeDs"));
                clusterConfig.setMaxCon(200);
                clusterConfig.setClusterType(ReplicaType.MASTER_SLAVE.name());
                clusterConfig.setSwitchType(ReplicaSwitchType.SWITCH.name());
                routerConfig.getClusters().add(clusterConfig);
            }
        }
    }

   public MycatRouterConfig getConfig();

    public void reportReplica(Map<String, List<String>> dsNames);


}
