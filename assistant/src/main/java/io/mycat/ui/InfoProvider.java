package io.mycat.ui;

import io.mycat.TableHandler;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.config.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface InfoProvider {
    List<LogicSchemaConfig> schemas();

    List<ClusterConfig> clusters();

    List<DatasourceConfig> datasources();

    public Optional<LogicSchemaConfig> getSchemaConfigByName(String schemaName);

    public Optional<Object> getTableConfigByName(String schemaName, String tableName);

    Optional<DatasourceConfig> getDatasourceConfigByPath(String name);

    Optional<ClusterConfig> getClusterConfigByPath(String name);

    String translate(String name);

    void deleteDatasource(String datasource);

    void deleteLogicalSchema(String schema);

    void saveCluster(ClusterConfig config);

    void saveDatasource(DatasourceConfig config);

    Connection createConnection();

    void saveSingleTable(String schemaName, String tableName, NormalTableConfig config);

    void saveGlobalTable(String schemaName, String tableName, GlobalTableConfig globalTableConfig);

    void deleteCluster(String cluster);

    void saveSchema(LogicSchemaConfig logicSchemaConfig);

    void deleteTable(String schema, String table);

    void saveShardingTable(String schemaName, String tableName, ShardingTableConfig config);

   void close();
}