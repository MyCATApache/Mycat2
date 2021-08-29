package io.mycat.config.controller;

import io.mycat.*;
import io.mycat.calcite.spm.DbPlanManagerPersistorImpl;
import io.mycat.calcite.spm.MemPlanCache;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.spm.UpdatePlanCache;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.commands.SqlResultSetService;
import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.util.NameMap;

import java.util.Collections;
import java.util.Map;

public class SchemaController {
    public static void update(Map<String, LogicSchemaConfig> schemaConfigs){
        clearCache();
        MetadataManager metadataManager = MetadataManager.createMetadataManager(schemaConfigs, "prototype", MetaClusterCurrent.wrapper(JdbcConnectionManager.class));
        MetaClusterCurrent.register(MetadataManager.class,metadataManager);
        MetaClusterCurrent.register(DrdsSqlCompiler.class, new DrdsSqlCompiler(new DrdsConfig() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                return metadataManager.getSchemaMap();
            }
        }));
    }

    private static void clearCache() {
        MetaClusterCurrent.register(UpdatePlanCache.class, new UpdatePlanCache());
        if (!MetaClusterCurrent.exist(SqlResultSetService.class)) {
            MetaClusterCurrent.register(SqlResultSetService.class, new SqlResultSetService());
        }else {
            MetaClusterCurrent.wrapper(SqlResultSetService.class).clear();
        }

        if (!MetaClusterCurrent.exist(MemPlanCache.class)) {
            DbPlanManagerPersistorImpl newDbPlanManagerPersistor = new DbPlanManagerPersistorImpl();
            MemPlanCache memPlanCache = new MemPlanCache((newDbPlanManagerPersistor));
            MetaClusterCurrent.register(MemPlanCache.class, memPlanCache);
            MetaClusterCurrent.register(QueryPlanner.class, new QueryPlanner(memPlanCache));
        } else {
            MemPlanCache memPlanCache = MetaClusterCurrent.wrapper(MemPlanCache.class);
            memPlanCache.clearCache();
        }
    }

    public static void addTable(CreateTableConfig createTableConfig){
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        String schemaName = createTableConfig.getSchemaName();
        String tableName = createTableConfig.getTableName();
        removeTable(schemaName, tableName);
        GlobalTableConfig globalTable = createTableConfig.getGlobalTable();
        if (globalTable != null) {
            metadataManager.addGlobalTable(schemaName, tableName, globalTable);
        }
        NormalTableConfig normalTable = createTableConfig.getNormalTable();
        if (normalTable != null) {
            metadataManager.addNormalTable(schemaName, tableName, normalTable);
        }
        ShardingTableConfig shardingTable = createTableConfig.getShardingTable();
        if (shardingTable != null) {
            metadataManager.addShardingTable(schemaName, tableName, shardingTable);
        }
        TableHandler table = metadataManager.getTable(schemaName, tableName);
        table.createPhysicalTables();
    }
    public static void removeTable(String schema,String table){
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.removeTable(schema, table);
        clearCache();
    }
    public static void addSchema(LogicSchemaConfig schemaConfig){
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.addSchema(schemaConfig);
    }
    public static void removeSchema(String schema){
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        MetaClusterCurrent.register(UpdatePlanCache.class, new UpdatePlanCache());
        metadataManager.removeSchema(schema);
        MemPlanCache memPlanCache = MetaClusterCurrent.wrapper(MemPlanCache.class);
        memPlanCache.clearCache();
    }
}
