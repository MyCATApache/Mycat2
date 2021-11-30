package io.mycat.ui;

import com.google.common.collect.ImmutableMap;
import io.mycat.*;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.*;
import io.mycat.connection.MycatLocalDriver;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.ui.InfoProvider;
import io.mycat.util.NameMap;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LocalInfoProvider implements InfoProvider {
    public LocalInfoProvider() {

    }

    @Override
    public List<LogicSchemaConfig> schemas() {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        return mycatRouterConfig.getSchemas();
    }

    @Override
    public List<ClusterConfig> clusters() {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        return mycatRouterConfig.getClusters();
    }

    @Override
    public List<DatasourceConfig> datasources() {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        return mycatRouterConfig.getDatasources();
    }

    @Override
    public Optional<LogicSchemaConfig> getSchemaConfigByName(String schemaName) {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        for (LogicSchemaConfig schema : mycatRouterConfig.getSchemas()) {
            if (schema.getSchemaName().equals(schemaName)) {
                return Optional.ofNullable(schema);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<Object> getTableConfigByName(String schemaName, String tableName) {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler table = metadataManager.getTable(schemaName, tableName);
        Object res = null;
        switch (table.getType()) {
            case SHARDING:
                res = ((ShardingTable)table).getTableConfig();
                break;
            case GLOBAL:
                res = ((GlobalTable)table).getTableConfig();
                break;
            case NORMAL:
                res = ((NormalTable)table).getTableConfig();
                break;
            case CUSTOM:
                break;
        }
        return Optional.ofNullable(res);
    }

    @Override
    public Optional<DatasourceConfig> getDatasourceConfigByPath(String name) {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        for (DatasourceConfig datasourceConfig : mycatRouterConfig.getDatasources()) {
            if (name.equals(datasourceConfig.getName())) {
                return Optional.ofNullable(datasourceConfig);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<ClusterConfig> getClusterConfigByPath(String path) {
        MycatRouterConfig mycatRouterConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        for (ClusterConfig cluster : mycatRouterConfig.getClusters()) {
            if (path.equals(cluster.getName())) {
                return Optional.ofNullable(cluster);
            }
        }
        return Optional.empty();


    }

    @Override
    public String translate(String name) {
        return map.get(name, false);
    }

    @Override
    @SneakyThrows
    public void deleteDatasource(String datasource) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.removeDatasource(datasource);
            ops.commit();
        }
    }

    @Override
    @SneakyThrows
    public void deleteLogicalSchema(String schema) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.dropSchema(schema);
            ops.commit();
        }
    }

    @Override
    @SneakyThrows
    public void saveCluster(ClusterConfig config) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.putReplica(config);
            ops.commit();
        }
    }

    @Override
    @SneakyThrows
    public void saveDatasource(DatasourceConfig config) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.putDatasource(config);
            ops.commit();
        }
    }

    @Override
    @SneakyThrows
    public Connection createConnection() {
            Class.forName(MycatLocalDriver.class.getCanonicalName());
        return DriverManager.getConnection("jdbc:mycat://localhost:80");

    }

    @Override
    @SneakyThrows
    public void saveSingleTable(String schemaName, String tableName, NormalTableConfig config) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.putNormalTable(schemaName,tableName,config);
            ops.commit();
        }
    }

    @Override
    public void saveGlobalTable(String schemaName, String tableName, GlobalTableConfig globalTableConfig) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        List<Partition> list = new ArrayList<>();
        List<GlobalBackEndTableInfoConfig> configList = globalTableConfig.getBroadcast();

        for (GlobalBackEndTableInfoConfig target : configList) {
            list.add(new BackendTableInfo(target.getTargetName(), schemaName, tableName));
        }

        metadataManager.addGlobalTable(schemaName, tableName, globalTableConfig, list);
    }

    @Override
    @SneakyThrows
    public void deleteCluster(String cluster) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.removeReplica(cluster);
            ops.commit();
        }
    }

    @Override
    @SneakyThrows
    public void saveSchema(LogicSchemaConfig logicSchemaConfig) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.putSchema(logicSchemaConfig);
            ops.commit();
        }
    }

    @Override
    @SneakyThrows
    public void deleteTable(String schema, String table) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.removeTable(schema,table);
            ops.commit();
        }
    }

    @Override
    @SneakyThrows
    public void saveShardingTable(String schemaName, String tableName, ShardingTableConfig config) {
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.putShardingTable(schemaName,tableName,config);
            ops.commit();
        }
    }

    @Override
    public void close() {

    }

    NameMap<String> map = NameMap.immutableCopyOf((ImmutableMap)
            ImmutableMap.builder()
                    .put("schemaName", "库名")
                    .put("defaultTargetName", "默认映射库目标")
                    .build());

}
