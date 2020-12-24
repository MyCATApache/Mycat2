package io.mycat;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.config.*;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;


public class MycatRouterConfigOps implements AutoCloseable {
    private MycatRouterConfig mycatRouterConfig;
    private final ConfigOps configOps;
    List<LogicSchemaConfig> schemas = null;
    List<ClusterConfig> clusters = null;
    List<UserConfig> users = null;
    List<SequenceConfig> sequences = null;
    List<DatasourceConfig> datasources = null;
    //    String prototype = null;
    UpdateType updateType = UpdateType.FULL;

    List<SqlCacheConfig> sqlCaches = null;
    SqlCacheConfig sqlCache = null;

    String tableName;
    String schemaName;


    public boolean isUpdateSchemas() {
        return schemas != null;
    }

    public boolean isUpdateClusters() {
        return clusters != null;
    }

    public boolean isUpdateUsers() {
        return users != null;
    }

    public boolean isUpdateSequences() {
        return sequences != null;
    }

    public boolean isUpdateSqlCaches() {
        return sqlCaches != null;
    }

    public boolean isUpdateDatasources() {
        return datasources != null;
    }

//    public boolean isUpdatePrototype() {
//        return prototype != null;
//    }

    public MycatRouterConfigOps(
            MycatRouterConfig mycatRouterConfig,
            ConfigOps configOps
    ) {
        this.mycatRouterConfig = mycatRouterConfig;
//        this.prototype = mycatRouterConfig.getPrototype();
        this.configOps = configOps;
    }


    public void addSchema(String schemaName, String targetName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        LogicSchemaConfig schemaConfig;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> schemaName.equals(i.getSchemaName())).findFirst();
        if (first.isPresent()) {
            first.get().setTargetName(targetName);
        } else {
            schemas.add(schemaConfig = new LogicSchemaConfig());
            schemaConfig.setSchemaName(schemaName);
        }
        updateType = UpdateType.ROUTER;
    }

    public void putSchema(LogicSchemaConfig schemaConfig) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i ->
                Objects.requireNonNull(schemaConfig.getSchemaName(), "schema name is null")
                        .equals(i.getSchemaName())).findFirst();
        first.ifPresent(schemas::remove);
        schemas.add(schemaConfig);
        updateType = UpdateType.ROUTER;
    }

    public void addTargetOnExistedSchema(String schemaName, String targetName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(i -> i.setTargetName(targetName));
        updateType = UpdateType.ROUTER;
    }


    public void dropSchema(String schemaName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(o -> {
            schemas.remove(o);
        });
        updateType = UpdateType.ROUTER;
    }


    public void putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        String defaultTarget = "prototype";
        putNormalTable(schemaName, tableName, sqlString, defaultTarget);
    }


    public NormalTableConfig putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString, String targetName) {

        NormalTableConfig normalTableConfig = new NormalTableConfig();
        normalTableConfig.setCreateTableSQL(sqlString.toString());
        normalTableConfig.setDataNode(NormalBackEndTableInfoConfig.builder()
                .targetName(targetName)
                .schemaName(schemaName)
                .tableName(tableName)
                .build());

        return putNormalTable(schemaName, tableName, normalTableConfig);
    }

    public NormalTableConfig putNormalTable(String schemaName, String tableName, NormalTableConfig normalTableConfig) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;

        LogicSchemaConfig logicSchemaConfig = schemas.stream()
                .filter(i -> i.getSchemaName().equals(schemaName))
                .findFirst().orElse(null);
        if (logicSchemaConfig == null) {
            throw new IllegalArgumentException("unknown:" + schemaName);
        }

        Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
        normalTables.put(tableName, normalTableConfig);


        updateType = UpdateType.CREATE_TABLE;
        this.tableName = tableName;
        this.schemaName = schemaName;
        return normalTableConfig;
    }

    public void putTable(CreateTableConfig createTableConfig) {
        String schemaName = createTableConfig.getSchemaName();
        String tableName = createTableConfig.getTableName();
        NormalTableConfig normalTable = createTableConfig.getNormalTable();
        GlobalTableConfig globalTable = createTableConfig.getGlobalTable();
        ShardingTableConfig shadingTable = createTableConfig.getShadingTable();

        if (normalTable != null) {
            putNormalTable(schemaName, tableName, normalTable);
        } else if (globalTable != null) {
            putGlobalTableConfig(schemaName, tableName, globalTable);
        } else if (shadingTable != null) {
            putShardingTable(schemaName, tableName, shadingTable);
        }
    }

    public GlobalTableConfig putGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        GlobalTableConfig globalTableConfig = getGlobalTableConfig(sqlString);
        return putGlobalTableConfig(schemaName, tableName, globalTableConfig);
    }

    public GlobalTableConfig putGlobalTableConfig(String schemaName, String tableName, GlobalTableConfig globalTableConfig) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        LogicSchemaConfig logicSchemaConfig = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst().orElse(null);

        if (logicSchemaConfig == null) {
            throw new IllegalArgumentException("unknown:" + schemaName);
        }

        Map<String, GlobalTableConfig> globalTableConfigMap = logicSchemaConfig.getGlobalTables();
        globalTableConfigMap.put(tableName, globalTableConfig);
        updateType = UpdateType.CREATE_TABLE;
        this.tableName = tableName;
        this.schemaName = schemaName;
        return globalTableConfig;
    }

    @NotNull
    private GlobalTableConfig getGlobalTableConfig(MySqlCreateTableStatement sqlString) {
        List<ClusterConfig> clusters = mycatRouterConfig.getClusters();
        List<String> allReplica = clusters.stream().map(i -> i.getName()).filter(i -> i.startsWith("c")).collect(Collectors.toList());
        GlobalTableConfig globalTableConfig = new GlobalTableConfig();
        globalTableConfig.setCreateTableSQL(sqlString.toString());
        globalTableConfig.setDataNodes(allReplica.stream()
                .map(i -> {
                    GlobalBackEndTableInfoConfig backEndTableInfoConfig = new GlobalBackEndTableInfoConfig();
                    backEndTableInfoConfig.setTargetName(i);
                    return backEndTableInfoConfig;
                }).collect(Collectors.toList()));
        return globalTableConfig;
    }


    public void removeTable(String schemaName, String tableName) {
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            logicSchemaConfig.getNormalTables().remove(tableName);
            logicSchemaConfig.getGlobalTables().remove(tableName);
            logicSchemaConfig.getShadingTables().remove(tableName);
            logicSchemaConfig.getCustomTables().remove(tableName);
        });
        updateType = UpdateType.DROP_TABLE;
        this.tableName = tableName;
        this.schemaName = schemaName;
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
                .function(ShardingFuntion.builder().name(name).clazz(aClass).properties((Map) properties).ranges((Map) ranges).build())
                .dataNode(Optional.ofNullable(dataNodes).map(i -> ShardingBackEndTableInfoConfig
                        .builder()
                        .schemaNames(dataNodes.get("schemaNames"))
                        .tableNames(dataNodes.get("tableNames"))
                        .targetNames(dataNodes.get("targetNames")).build())
                        .orElse(null))
                .build();

        return putShardingTable(schemaName, tableName, config);
    }

    public ShardingTableConfig putShardingTable(String schemaName, String tableName, ShardingTableConfig config) {
        removeTable(schemaName, tableName);
        this.schemas = mycatRouterConfig.getSchemas();
        List<LogicSchemaConfig> schemas = this.schemas;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equals(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            Map<String, ShardingTableConfig> shadingTables = logicSchemaConfig.getShadingTables();
            shadingTables.put(tableName, config);
        });
        updateType = UpdateType.CREATE_TABLE;
        this.tableName = tableName;
        this.schemaName = schemaName;
        return config;
    }


    public ShardingTableConfig putHashTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
        ShardingTableConfig config = builder
                .createTableSQL(tableStatement.toString())
                .function(ShardingFuntion.builder().properties((Map) infos).build())
                .build();
        return putShardingTable(schemaName, tableName, config);
    }

    public void putUser(String username, String password, String ip, String transactionType) {
        UserConfig userConfig = UserConfig.builder()
                .username(username)
                .password(password)
                .ip(ip)
                .transactionType(transactionType)
                .build();
        putUser(userConfig);
    }

    public void putUser(UserConfig userConfig) {
        this.users = mycatRouterConfig.getUsers();
        this.users.stream().filter(u -> u.getUsername().equals(userConfig.getUsername()))
                .findFirst().ifPresent(find -> this.users.remove(find));
        users.add(userConfig);
        updateType = UpdateType.USER;
    }


    public void deleteUser(String username) {
        this.users = mycatRouterConfig.getUsers();
        users.stream().filter(i -> username.equals(i.getUsername()))
                .findFirst().ifPresent(i -> users.remove(i));
        updateType = UpdateType.USER;
    }

    public void putSequence(SequenceConfig sequenceConfig) {
        this.sequences = mycatRouterConfig.getSequences();
        sequences.stream().filter(i -> i.getName().equals(sequenceConfig.getName()))
                .findFirst().ifPresent(s -> sequences.remove(s));
        sequences.add(sequenceConfig);
        updateType = UpdateType.SEQUENCE;
    }


    public void removeSequenceByName(String name) {
        this.sequences = mycatRouterConfig.getSequences();
        sequences.stream()
                .filter(i -> name.equals(i.getName())).findFirst()
                .ifPresent(i -> sequences.remove(i));
        updateType = UpdateType.SEQUENCE;
    }


    public void putDatasource(DatasourceConfig datasourceConfig) {
        this.datasources = mycatRouterConfig.getDatasources();
        Optional<DatasourceConfig> first = datasources.stream().filter(i -> datasourceConfig.getName().equals(i.getName())).findFirst();
        first.ifPresent(config -> datasources.remove(config));
        datasources.add(datasourceConfig);
        updateType = UpdateType.FULL;
    }

    public void removeDatasource(String datasourceName) {
        this.datasources = mycatRouterConfig.getDatasources();
        Optional<DatasourceConfig> first = datasources.stream().filter(i -> datasourceName.equals(i.getName())).findFirst();
        first.ifPresent(datasources::remove);
        updateType = UpdateType.FULL;
    }

    public void putReplica(ClusterConfig clusterConfig) {
        this.clusters = mycatRouterConfig.getClusters();
        Optional<ClusterConfig> first = this.clusters.stream().filter(i -> clusterConfig.getName().equals(i.getName())).findFirst();
        first.ifPresent(clusters::remove);
        clusters.add(clusterConfig);
        updateType = UpdateType.FULL;
    }

    public void removeReplica(String replicaName) {
        this.clusters = mycatRouterConfig.getClusters();
        List<ClusterConfig> clusters = this.clusters;
        Optional<ClusterConfig> first = clusters.stream().filter(i -> replicaName.equals(i.getName())).findFirst();
        first.ifPresent(clusters::remove);
        updateType = UpdateType.FULL;
    }

    public void putSqlCache(SqlCacheConfig currentSqlCacheConfig) {
        this.sqlCaches = mycatRouterConfig.getSqlCacheConfigs();
        Optional<SqlCacheConfig> first = this.sqlCaches.stream().filter(i -> currentSqlCacheConfig.getName().equals(i.getName())).findFirst();
        first.ifPresent(o -> {
            sqlCaches.remove(o);
            this.sqlCache = currentSqlCacheConfig;
        });
        this.sqlCaches.add(currentSqlCacheConfig);
        this.sqlCache = currentSqlCacheConfig;
        updateType = UpdateType.CREATE_SQL_CACHE;
    }

    public void removeSqlCache(String cacheName) {
        Optional<SqlCacheConfig> first = mycatRouterConfig.getSqlCacheConfigs()
                .stream().filter(i -> cacheName.equals(i.getName())).findFirst();
        if (!first.isPresent()) {
            return;
        }
        this.sqlCaches = mycatRouterConfig.getSqlCacheConfigs();
        first.ifPresent(o -> {
            sqlCaches.remove(o);
            this.sqlCache = o;
        });
        updateType = UpdateType.DROP_SQL_CACHE;
    }

    public List<ClusterConfig> getClusters() {
        return clusters;
    }

    public List<UserConfig> getUsers() {
        return users;
    }

    public List<SequenceConfig> getSequences() {
        return sequences;
    }

    public List<DatasourceConfig> getDatasources() {
        return datasources;
    }

    public MycatRouterConfig getMycatRouterConfig() {
        return mycatRouterConfig;
    }

    public List<LogicSchemaConfig> getSchemas() {
        return schemas;
    }

    public List<SqlCacheConfig> getSqlCaches() {
        return sqlCaches;
    }

    //
//    public String getPrototype() {
//        return prototype;
//    }


    public MycatRouterConfig currentConfig() {
        return mycatRouterConfig;
    }


    public void commit() throws Exception {
        this.configOps.commit(this);
    }

    public void close() {
        this.configOps.close();
    }

    public UpdateType getUpdateType() {
        return updateType;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void putHashTable(String schemaName, String tableName, MySqlCreateTableStatement createTableSql) {
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

        putHashTable(schemaName, tableName, createTableSql, properties);
    }

    public void reset() {
        MycatRouterConfig newMycatRouterConfig = new MycatRouterConfig();
        newMycatRouterConfig.setSchemas(this.mycatRouterConfig.getSchemas().stream()
                .filter(i -> "mysql".equals(i.getSchemaName())).collect(Collectors.toList()));
        this.mycatRouterConfig = newMycatRouterConfig;
        FileMetadataStorageManager.defaultConfig(this.mycatRouterConfig);
        this.updateType = UpdateType.RESET;
        this.schemas = this.mycatRouterConfig.getSchemas();
        this.clusters = this.mycatRouterConfig.getClusters();
        this.users = this.mycatRouterConfig.getUsers();
        this.sequences = this.mycatRouterConfig.getSequences();
        this.datasources = this.mycatRouterConfig.getDatasources();
        this.sqlCaches = this.mycatRouterConfig.getSqlCacheConfigs();
    }
}
