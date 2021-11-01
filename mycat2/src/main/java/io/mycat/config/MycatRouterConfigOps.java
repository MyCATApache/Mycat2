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
package io.mycat.config;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.impl.LocalXaMemoryRepositoryImpl;
import cn.mycat.vertx.xa.impl.XaLogImpl;
import com.alibaba.druid.sql.MycatSQLUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.*;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.calcite.spm.DbPlanManagerPersistorImpl;
import io.mycat.calcite.spm.MemPlanCache;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.spm.UpdatePlanCache;
import io.mycat.calcite.table.DualCustomTableHandler;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.commands.MycatMySQLManagerImpl;
import io.mycat.commands.SqlResultSetService;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.monitor.MonitorReplicaSelectorManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.replica.ReplicaType;
import io.mycat.sqlhandler.config.KV;
import io.mycat.sqlhandler.config.StorageManager;
import io.mycat.sqlhandler.config.UpdateSet;
import io.mycat.statistic.StatisticCenter;
import io.mycat.util.JsonUtil;
import io.mycat.util.NameMap;
import io.vertx.core.json.Json;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;


public class MycatRouterConfigOps implements AutoCloseable, ConfigOps {
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatRouterConfigOps.class);
    final MycatRouterConfig original;
    final StorageManager storageManager;
    private Options options;
    MycatRouterConfig newConfig;

    static boolean init = true;


    public MycatRouterConfigOps(MycatRouterConfig original, StorageManager storageManager, Options options) {
        this.original = original;
        this.storageManager = storageManager;
        this.options = options;
        this.newConfig = Json.decodeValue(Json.encode(original), MycatRouterConfig.class);
    }

    public MycatRouterConfigOps(MycatRouterConfig original, StorageManager storageManager, Options options, MycatRouterConfig newConfig) {
        this.original = original;
        this.storageManager = storageManager;
        this.options = options;
        this.newConfig = newConfig;
    }

    public static boolean isInit() {
        boolean init = MycatRouterConfigOps.init;
        if (init) {
            MycatRouterConfigOps.init = false;
        }
        return init;
    }

    @Override
    public void addSchema(String schemaName, String targetName) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        LogicSchemaConfig schemaConfig;
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> schemaName.equalsIgnoreCase(i.getSchemaName())).findFirst();
        if (first.isPresent()) {
            first.get().setTargetName(targetName);
        } else {
            schemas.add(schemaConfig = new LogicSchemaConfig());
            schemaConfig.setSchemaName(schemaName);
            schemaConfig.setTargetName(targetName);
        }
    }

    @Override
    public void putSchema(LogicSchemaConfig schemaConfig) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i ->
                Objects.requireNonNull(schemaConfig.getSchemaName(), "schema name is null")
                        .equalsIgnoreCase(i.getSchemaName())).findFirst();
        first.ifPresent(schemas::remove);
        schemas.add(schemaConfig);
    }

    @Override
    public void dropSchema(String schemaName) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equalsIgnoreCase(schemaName)).findFirst();
        first.ifPresent(o -> {
            schemas.remove(o);
        });
    }

    @Override
    public void putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equalsIgnoreCase(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            String defaultTarget = Optional.ofNullable(logicSchemaConfig.getTargetName()).orElse(MetadataManager.getPrototype());
            putNormalTable(schemaName, tableName, sqlString, defaultTarget);
        });
        options.createSchemaName = schemaName;
        options.createTableName = tableName;
    }

    @Override
    public NormalTableConfig putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString, String targetName) {
        NormalTableConfig normalTableConfig = new NormalTableConfig();
        normalTableConfig.setCreateTableSQL(sqlString.toString());
        normalTableConfig.setLocality(NormalBackEndTableInfoConfig.builder()
                .targetName(targetName)
                .schemaName(schemaName)
                .tableName(tableName)
                .build());

        return putNormalTable(schemaName, tableName, normalTableConfig);
    }

    @Override
    public NormalTableConfig putNormalTable(String schemaName, String tableName, NormalTableConfig normalTableConfig) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();

        LogicSchemaConfig logicSchemaConfig = schemas.stream()
                .filter(i -> i.getSchemaName().equalsIgnoreCase(schemaName))
                .findFirst().orElse(null);
        if (logicSchemaConfig == null) {
            throw new IllegalArgumentException("unknown:" + schemaName);
        }

        Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
        normalTables.put(tableName, normalTableConfig);
        options.createSchemaName = schemaName;
        options.createTableName = tableName;
        return normalTableConfig;
    }

    @Override
    public void putTable(CreateTableConfig createTableConfig) {
        String schemaName = createTableConfig.getSchemaName();
        String tableName = createTableConfig.getTableName();
        NormalTableConfig normalTable = createTableConfig.getNormalTable();
        GlobalTableConfig globalTable = createTableConfig.getGlobalTable();
        ShardingTableConfig shardingTable = createTableConfig.getShardingTable();

        if (normalTable != null) {
            putNormalTable(schemaName, tableName, normalTable);
        } else if (globalTable != null) {
            putGlobalTableConfig(schemaName, tableName, globalTable);
        } else if (shardingTable != null) {
            putShardingTable(schemaName, tableName, shardingTable);
        }
    }

    @Override
    public GlobalTableConfig putGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        GlobalTableConfig globalTableConfig = getGlobalTableConfig(sqlString);
        return putGlobalTableConfig(schemaName, tableName, globalTableConfig);

    }

    @Override
    public GlobalTableConfig putGlobalTableConfig(String schemaName, String tableName, GlobalTableConfig globalTableConfig) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        LogicSchemaConfig logicSchemaConfig = schemas.stream().filter(i -> i.getSchemaName().equalsIgnoreCase(schemaName)).findFirst().orElse(null);

        if (logicSchemaConfig == null) {
            throw new IllegalArgumentException("unknown:" + schemaName);
        }

        Map<String, GlobalTableConfig> globalTableConfigMap = logicSchemaConfig.getGlobalTables();
        globalTableConfigMap.put(tableName, globalTableConfig);
        options.createSchemaName = schemaName;
        options.createTableName = tableName;
        return globalTableConfig;
    }

    @NotNull
    private GlobalTableConfig getGlobalTableConfig(MySqlCreateTableStatement sqlString) {
        List<ClusterConfig> clusters = newConfig.getClusters();
        List<String> allReplica = clusters.stream().map(i -> i.getName()).filter(i -> i.startsWith("c")).collect(Collectors.toList());
        GlobalTableConfig globalTableConfig = new GlobalTableConfig();
        globalTableConfig.setCreateTableSQL(sqlString.toString());
        globalTableConfig.setBroadcast(allReplica.stream()
                .map(i -> {
                    GlobalBackEndTableInfoConfig backEndTableInfoConfig = new GlobalBackEndTableInfoConfig();
                    backEndTableInfoConfig.setTargetName(i);
                    return backEndTableInfoConfig;
                }).collect(Collectors.toList()));
        return globalTableConfig;
    }


    @Override
    public void removeTable(String schemaNameArg, String tableNameArg) {
        String schemaName = SQLUtils.normalize(schemaNameArg);
        String tableName = SQLUtils.normalize(tableNameArg);
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equalsIgnoreCase(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            logicSchemaConfig.getNormalTables().remove(tableName);
            logicSchemaConfig.getGlobalTables().remove(tableName);
            logicSchemaConfig.getShardingTables().remove(tableName);
            logicSchemaConfig.getCustomTables().remove(tableName);
        });
    }

    @Override
    public ShardingTableConfig putRangeTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        Map<String, String> ranges = (Map) infos.get("ranges");
        Map<String, String> dataNodes = (Map) Optional.ofNullable(infos.get("dataNodes")).orElseGet(() -> infos.get("partition"));
        Map<String, String> properties = (Map) infos.get("properties");
        String aClass = (String) (infos.get("class"));
        String name = (String) (infos.get("name"));
        ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
        ShardingTableConfig config = builder
                .createTableSQL(tableStatement.toString())
                .function(ShardingFunction.builder().name(name).clazz(aClass).properties((Map) properties).ranges((Map) ranges).build())
                .partition(Optional.ofNullable(dataNodes).map(i -> ShardingBackEndTableInfoConfig
                                .builder()
                                .schemaNames(dataNodes.get("schemaNames"))
                                .tableNames(dataNodes.get("tableNames"))
                                .targetNames(dataNodes.get("targetNames")).build())
                        .orElse(null))
                .build();

        return putShardingTable(schemaName, tableName, config);
    }

    @Override
    public ShardingTableConfig putShardingTable(String schemaName, String tableName, ShardingTableConfig config) {
        removeTable(schemaName, tableName);
        Map<String, ShardingTableConfig> indexTables
                = Optional.ofNullable(config.getShardingIndexTables()).orElse(Collections.emptyMap());
        for (Map.Entry<String, ShardingTableConfig> entry : indexTables.entrySet()) {
            removeTable(schemaName, entry.getKey());
        }
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> i.getSchemaName().equalsIgnoreCase(schemaName)).findFirst();
        first.ifPresent(logicSchemaConfig -> {
            Map<String, ShardingTableConfig> shardingTables = logicSchemaConfig.getShardingTables();
            shardingTables.put(tableName, config);
        });
        options.createSchemaName = schemaName;
        options.createTableName = tableName;
        return config;
    }

    @Override
    public ShardingTableConfig putHashTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        NameMap<SQLColumnDefinition> columnMap = NameMap.immutableCopyOf(tableStatement.getColumnDefinitions().stream()
                .collect(Collectors.toMap(k -> SQLUtils.normalize(k.getColumnName()), v -> v)));

        Map<String, ShardingTableConfig> indexTableConfigs = new HashMap<>();
        MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) tableStatement.getTableElementList().stream().filter(i -> i instanceof MySqlPrimaryKey).findFirst().orElse(null);
        for (SQLTableElement sqlTableElement : tableStatement.getTableElementList()) {
            if (sqlTableElement instanceof MySqlTableIndex) {
                MySqlTableIndex element = (MySqlTableIndex) sqlTableElement;
                if (!element.isGlobal()) {
                    continue;
                }
                SQLIndexDefinition indexDefinition = element.getIndexDefinition();
                MySqlCreateTableStatement indexCreateTableStatement = new MySqlCreateTableStatement();
                indexCreateTableStatement.setIfNotExiists(true);

                String indexTableName = tableName + "_" + SQLUtils.normalize(indexDefinition.getName().getSimpleName());
                indexCreateTableStatement.setTableName(indexTableName);
                indexCreateTableStatement.setSchema(schemaName);
                for (SQLSelectOrderByItem indexColumn : indexDefinition.getColumns()) {
                    indexCreateTableStatement.addColumn(columnMap.get(SQLUtils.normalize(indexColumn.getExpr().toString())));
                }
                for (SQLName sqlName : indexDefinition.getCovering()) {
                    indexCreateTableStatement.addColumn(columnMap.get(SQLUtils.normalize(sqlName.toString())));
                }
                if (primaryKey != null) {
                    indexCreateTableStatement.getTableElementList().add(primaryKey);
                }
                indexCreateTableStatement.setDbPartitionBy(indexDefinition.getDbPartitionBy());
                indexCreateTableStatement.setTablePartitionBy(indexDefinition.getTbPartitionBy());

                indexCreateTableStatement.setDbPartitions(indexCreateTableStatement.getDbPartitions());
                indexCreateTableStatement.setTablePartitions(indexDefinition.getTbPartitions());
                Map<String, Object> autoHashProperties = getAutoHashProperties(indexCreateTableStatement);

                ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
                ShardingTableConfig config = builder
                        .createTableSQL(MycatSQLUtils.toString(indexCreateTableStatement))
                        .function(ShardingFunction.builder().properties(autoHashProperties).build())
                        .build();

                indexTableConfigs.put(indexTableName, config);
            }
        }

        ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
        ShardingTableConfig config = builder
                .createTableSQL(MycatSQLUtils.toString(tableStatement))
                .function(ShardingFunction.builder().properties((Map) infos).build())
                .shardingIndexTables(indexTableConfigs)
                .build();
        return putShardingTable(schemaName, tableName, config);
    }

    @Override
    public void putUser(String username, String password, String ip, String transactionType) {
        UserConfig userConfig = UserConfig.builder()
                .username(username)
                .password(password)
                .ip(ip)
                .transactionType(transactionType)
                .build();
        putUser(userConfig);
    }

    @Override
    public void putUser(UserConfig userConfig) {
        List<UserConfig> users = newConfig.getUsers();
        users.stream().filter(u -> u.getUsername().equalsIgnoreCase(userConfig.getUsername()))
                .findFirst().ifPresent(find -> users.remove(find));
        users.add(userConfig);
    }

    @Override
    public void deleteUser(String username) {
        List<UserConfig> users = newConfig.getUsers();
        users.stream().filter(i -> username.equalsIgnoreCase(i.getUsername()))
                .findFirst().ifPresent(i -> users.remove(i));
    }

    @Override
    public void putSequence(SequenceConfig sequenceConfig) {
        List<SequenceConfig> sequences = newConfig.getSequences();
        sequences.stream().filter(i -> i.getName().equalsIgnoreCase(sequenceConfig.getName()))
                .findFirst().ifPresent(s -> sequences.remove(s));
        sequences.add(sequenceConfig);
    }

    @Override
    public void removeSequenceByName(String name) {
        List<SequenceConfig> sequences = newConfig.getSequences();
        sequences.stream()
                .filter(i -> name.equalsIgnoreCase(i.getName())).findFirst()
                .ifPresent(i -> sequences.remove(i));
    }

    @Override
    public void putDatasource(DatasourceConfig datasourceConfig) {
        List<DatasourceConfig> datasources = newConfig.getDatasources();
        Optional<DatasourceConfig> first = datasources.stream().filter(i -> datasourceConfig.getName().equalsIgnoreCase(i.getName())).findFirst();
        first.ifPresent(config -> datasources.remove(config));
        datasources.add(datasourceConfig);
    }

    @Override
    public void removeDatasource(String datasourceName) {
        List<DatasourceConfig> datasources = newConfig.getDatasources();
        Optional<DatasourceConfig> first = Optional.empty();
        for (DatasourceConfig i : datasources) {
            if (datasourceName.equalsIgnoreCase(i.getName())) {
                first = Optional.of(i);
                break;
            }
        }
        first.ifPresent(datasources::remove);
    }

    @Override
    public void putReplica(ClusterConfig clusterConfig) {
        List<ClusterConfig> clusters = newConfig.getClusters();
        Optional<ClusterConfig> first = clusters.stream().filter(i -> clusterConfig.getName().equalsIgnoreCase(i.getName())).findFirst();
        first.ifPresent(clusters::remove);
        clusters.add(clusterConfig);
    }

    @Override
    public void removeReplica(String replicaName) {
        List<ClusterConfig> clusters = newConfig.getClusters();
        Optional<ClusterConfig> first = clusters.stream().filter(i -> replicaName.equalsIgnoreCase(i.getName())).findFirst();
        first.ifPresent(clusters::remove);
    }

    @Override
    public void putSqlCache(SqlCacheConfig currentSqlCacheConfig) {
        List<SqlCacheConfig> sqlCaches = newConfig.getSqlCacheConfigs();
        Optional<SqlCacheConfig> first = sqlCaches.stream().filter(i -> currentSqlCacheConfig.getName().equalsIgnoreCase(i.getName())).findFirst();
        first.ifPresent(o -> {
            sqlCaches.remove(o);
        });
        sqlCaches.add(currentSqlCacheConfig);
    }

    @Override
    public void removeSqlCache(String cacheName) {
        Optional<SqlCacheConfig> first = newConfig.getSqlCacheConfigs()
                .stream().filter(i -> cacheName.equalsIgnoreCase(i.getName())).findFirst();
        if (!first.isPresent()) {
            return;
        }
        List<SqlCacheConfig> sqlCaches = newConfig.getSqlCacheConfigs();
        first.ifPresent(o -> {
            sqlCaches.remove(o);
        });
    }

    public List<TableHandler> getCreateTables(MycatRouterConfig original, MycatRouterConfig newConfig, MetadataManager metadataManager) {
        List<TableHandler> res = new ArrayList<>();
        List<LogicSchemaConfig> orginalSchemas = original.getSchemas();
        List<LogicSchemaConfig> newSchemas = newConfig.getSchemas();

        Map<String, LogicSchemaConfig> oldSchemaConfigMap = orginalSchemas.stream().collect(Collectors.toMap(k -> k.getSchemaName(), v -> v));
        for (LogicSchemaConfig newSchema : newSchemas) {
            if (oldSchemaConfigMap.containsKey(newSchema.getSchemaName())) {
                LogicSchemaConfig oldSchema = oldSchemaConfigMap.get(newSchema.getSchemaName());
                Set<String> strings = newSchema.tableNames();
                for (String s : strings) {
                    if (!oldSchema.findTable(s).isPresent()) {
                        res.add(metadataManager.getTable(newSchema.getSchemaName(), s));
                    }
                }
            } else {
                for (String s : newSchema.tableNames()) {
                    res.add(metadataManager.getTable(newSchema.getSchemaName(), s));
                }
            }
        }
        return res;
    }


    @Override
    public void commit() throws Exception {
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        boolean init = isInit();
        MycatRouterConfig newConfig = this.newConfig;
        defaultConfig(newConfig);
        newConfig.fixPrototypeTargetName();
        if (!newConfig.containsPrototypeTargetName()) {
            throw new UnsupportedOperationException();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(JsonUtil.toJson(this.original));
            LOGGER.debug("===========================================change to ===========================================");
            LOGGER.debug(JsonUtil.toJson(newConfig));
        }
        UpdateSet<LogicSchemaConfig> schemaConfigUpdateSet = UpdateSet.create(newConfig.getSchemas(), original.getSchemas());
        UpdateSet<ClusterConfig> clusterConfigUpdateSet = UpdateSet.create(newConfig.getClusters(), original.getClusters());
        UpdateSet<DatasourceConfig> datasourceConfigUpdateSet = UpdateSet.create(newConfig.getDatasources(), original.getDatasources());
        UpdateSet<SequenceConfig> sequenceConfigUpdateSet = UpdateSet.create(newConfig.getSequences(), original.getSequences());
        UpdateSet<SqlCacheConfig> sqlCacheConfigUpdateSet = UpdateSet.create(newConfig.getSqlCacheConfigs(), original.getSqlCacheConfigs());
        UpdateSet<UserConfig> userConfigUpdateSet = UpdateSet.create(newConfig.getUsers(), original.getUsers());


        List<Resource> resourceList = new ArrayList<>();
        boolean connectionUpdateSuccess = false;
        boolean allSuccess = false;
        try {

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
            if (MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
                ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
                replicaSelectorManager.stop();
            }
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            Resource<JdbcConnectionManager> jdbcConnectionManager = getJdbcConnectionManager(datasourceConfigUpdateSet);
            resourceList.add(jdbcConnectionManager);

            Resource<DatasourceConfigProvider> datasourceConfigProvider = getDatasourceConfigProvider(datasourceConfigUpdateSet);
            resourceList.add(datasourceConfigProvider);

            Resource<MySQLManager> mycatMySQLManager = getMycatMySQLManager(datasourceConfigUpdateSet);
            resourceList.add(mycatMySQLManager);

            Resource<ReplicaSelectorManager> replicaSelectorManager = getReplicaSelectorManager(clusterConfigUpdateSet,
                    datasourceConfigUpdateSet, jdbcConnectionManager);
            resourceList.add(replicaSelectorManager);

            JdbcConnectionManager jdbcConnectionManager1 = jdbcConnectionManager.get();


            MetaClusterCurrent.register(JdbcConnectionManager.class, jdbcConnectionManager.get());
            MetaClusterCurrent.register(ConnectionManager.class, jdbcConnectionManager.get());
            resourceList.add(jdbcConnectionManager);

            MetaClusterCurrent.register(DatasourceConfigProvider.class, datasourceConfigProvider.get());
            MetaClusterCurrent.register(ReplicaSelectorManager.class, replicaSelectorManager.get());
            MetaClusterCurrent.register(MySQLManager.class, mycatMySQLManager.get());

            testPrototype(jdbcConnectionManager1);

            //////////////////////////////////////////////////////////////////////////////////////////////////////////
            resourceList.clear();
            connectionUpdateSuccess = true;
            //////////////////////////////////////////////////////////////////////////////////////////////////////////
            Map<Class, Object> context = MetaClusterCurrent.copyContext();
            Resource<Authenticator> authenticator = getAuthenticator(userConfigUpdateSet);
            resourceList.add(authenticator);

            Resource<SqlResultSetService> sqlResultSetService = getSqlResultSetService(sqlCacheConfigUpdateSet);
            resourceList.add(sqlResultSetService);

            Resource<SequenceGenerator> sequenceGenerator = getSequenceGenerator(sequenceConfigUpdateSet);
            resourceList.add(sequenceGenerator);
            PrototypeService prototypeService ;
            context.put(PrototypeService.class,  prototypeService =  new PrototypeService());

            Resource<MetadataManager> metadataManager = getMetadataManager(schemaConfigUpdateSet, prototypeService);
            resourceList.add(metadataManager);

            Resource<DrdsSqlCompiler> drdsSqlCompiler = getDrdsSqlCompiler(schemaConfigUpdateSet, metadataManager);
            resourceList.add(drdsSqlCompiler);

            StatisticCenter statisticCenter = new StatisticCenter();
            Resource<MysqlVariableService> mysqlVariableService = getMysqlVariableService(jdbcConnectionManager);
            resourceList.add(mysqlVariableService);

            Resource<QueryPlanner> queryPlanner = getQueryPlanner(schemaConfigUpdateSet);
            resourceList.add(queryPlanner);

            Resource<XaLog> xaLog = getXaLog(serverConfig, mycatMySQLManager);
            resourceList.add(xaLog);

            Resource<UpdatePlanCache> updatePlanCache = getUpdatePlanCache(schemaConfigUpdateSet);
            resourceList.add(updatePlanCache);

            context.put(JdbcConnectionManager.class, jdbcConnectionManager.get());
            context.put(ConnectionManager.class, jdbcConnectionManager.get());
            context.put(Authenticator.class, authenticator.get());
            context.put(SqlResultSetService.class, sqlResultSetService.get());
            context.put(SequenceGenerator.class, sequenceGenerator.get());
            context.put(DatasourceConfigProvider.class, datasourceConfigProvider.get());
            context.put(ReplicaSelectorManager.class, replicaSelectorManager.get());
            context.put(MySQLManager.class, mycatMySQLManager.get());
            context.put(DrdsSqlCompiler.class, drdsSqlCompiler.get());
            context.put(MysqlVariableService.class, mysqlVariableService.get());
            context.put(XaLog.class, xaLog.get());
            context.put(UpdatePlanCache.class, updatePlanCache.get());
            context.put(QueryPlanner.class, queryPlanner.get());
            context.put(StatisticCenter.class, statisticCenter);
            context.put(MycatRouterConfig.class, newConfig);
            context.put(MetadataManager.class, metadataManager.get());
            MetaClusterCurrent.register(context);


            allSuccess = true;

            if (init) {
                recoveryXA();
                DbPlanManagerPersistorImpl dbPlanManagerPersistor = new DbPlanManagerPersistorImpl();
                dbPlanManagerPersistor.checkStore();
            }
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////
        } finally {
            if (!allSuccess) {
                resourceList.forEach(c -> c.giveup());
            }
            if (MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
                ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
                replicaSelectorManager.start();
            }
        }
        if (options.createSchemaName != null) {
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            TableHandler table = metadataManager.getTable(options.createSchemaName, options.createTableName);
            if (table != null) {
                table.createPhysicalTables();
            }
        }
        if (options.persistence) {
            persistence(schemaConfigUpdateSet, clusterConfigUpdateSet, datasourceConfigUpdateSet, sequenceConfigUpdateSet, sqlCacheConfigUpdateSet, userConfigUpdateSet);
        }

    }

    private void testPrototype(JdbcConnectionManager jdbcConnectionManager1) {
        try (DefaultConnection connection = jdbcConnectionManager1.getConnection(MetadataManager.getPrototype())) {

        }
    }

    @NotNull
    private Resource<UpdatePlanCache> getUpdatePlanCache(UpdateSet<LogicSchemaConfig> schemaConfigUpdateSet) {
        if (schemaConfigUpdateSet.isEmpty() && MetaClusterCurrent.exist(UpdatePlanCache.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(UpdatePlanCache.class), true);
        }
        return Resource.of(new UpdatePlanCache(), false);
    }

    @NotNull
    private Resource<XaLog> getXaLog(ServerConfig serverConfig, Resource<MySQLManager> mycatMySQLManagerResource) {
        if (MetaClusterCurrent.exist(XaLog.class) && mycatMySQLManagerResource.isBorrow()) {
            return Resource.of(MetaClusterCurrent.wrapper(XaLog.class), true);
        }
        MySQLManager mycatMySQLManager = mycatMySQLManagerResource.get();
        LocalXaMemoryRepositoryImpl localXaMemoryRepository = LocalXaMemoryRepositoryImpl.createLocalXaMemoryRepository(() -> mycatMySQLManager);
        XaLog xaLog = new XaLogImpl(localXaMemoryRepository, serverConfig.getMycatId(),
                Objects.requireNonNull(mycatMySQLManager));
        return Resource.of(xaLog, false);
    }

    @NotNull
    private Resource<MySQLManager> getMycatMySQLManager(UpdateSet<DatasourceConfig> datasourceConfigUpdateSet) {
        if (MetaClusterCurrent.exist(MySQLManager.class) && datasourceConfigUpdateSet.isEmpty()) {
            return Resource.of(MetaClusterCurrent.wrapper(MySQLManager.class), true);
        }
        return Resource.of(new MycatMySQLManagerImpl(datasourceConfigUpdateSet.getTargetAsList()), false);
    }

    @NotNull
    private Resource<QueryPlanner> getQueryPlanner(UpdateSet<LogicSchemaConfig> schemaConfigUpdateSet) {
        if (schemaConfigUpdateSet.isEmpty() && MetaClusterCurrent.exist(QueryPlanner.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(QueryPlanner.class), true);
        }
        DbPlanManagerPersistorImpl newDbPlanManagerPersistor = new DbPlanManagerPersistorImpl();
        return Resource.of(new QueryPlanner(new MemPlanCache((newDbPlanManagerPersistor))), false);
    }

    @NotNull
    private Resource<DrdsSqlCompiler> getDrdsSqlCompiler(UpdateSet<LogicSchemaConfig> schemaConfigUpdateSet, Resource<MetadataManager> metadataManagerResource) {
        if (MetaClusterCurrent.exist(DrdsSqlCompiler.class) && schemaConfigUpdateSet.isEmpty()) {
            return Resource.of(MetaClusterCurrent.wrapper(DrdsSqlCompiler.class), true);
        }
        MetadataManager manager = metadataManagerResource.get();
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        DrdsSqlCompiler.RBO_BKA_JOIN = serverConfig.isBkaJoin();
        DrdsSqlCompiler.RBO_MERGE_JOIN = serverConfig.isSortMergeJoin();
        DrdsSqlCompiler.BKA_JOIN_LEFT_ROW_COUNT_LIMIT = serverConfig.getBkaJoinLeftRowCountLimit();
        return Resource.of(new DrdsSqlCompiler(new DrdsConfig() {
            @Override
            public NameMap<SchemaHandler> schemas() {
                return manager.getSchemaMap();
            }
        }), false);
    }

    @NotNull
    private Resource<MetadataManager> getMetadataManager(UpdateSet<LogicSchemaConfig> schemaConfigUpdateSet, PrototypeService prototypeService) {
        if (MetaClusterCurrent.exist(MetadataManager.class)) {
            if (schemaConfigUpdateSet.isEmpty()) {
                return Resource.of(MetaClusterCurrent.wrapper(MetadataManager.class), true);
            } else if (schemaConfigUpdateSet.getDelete().isEmpty()&&!schemaConfigUpdateSet.getCreate().isEmpty()){
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                for (LogicSchemaConfig logicSchemaConfig : schemaConfigUpdateSet.getCreate()) {
                    metadataManager.addSchema(logicSchemaConfig);
                }
                return Resource.of(MetaClusterCurrent.wrapper(MetadataManager.class), true);
            }
        }
        return Resource.of(MetadataManager.createMetadataManager(schemaConfigUpdateSet.getTargetAsMap(), prototypeService), false);
    }

    @NotNull
    private Resource<MysqlVariableService> getMysqlVariableService(
            Resource<JdbcConnectionManager> jdbcConnectionManagerResource) {
        if (MetaClusterCurrent.exist(MysqlVariableService.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(MysqlVariableService.class), true);
        }
        return Resource.of(new MysqlVariableServiceImpl(jdbcConnectionManagerResource.get()), false);
    }

    private void recoveryXA() {
        MycatRouterConfig curConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        boolean allMatchMySQL = curConfig.getDatasources().stream().allMatch(s -> "mysql".equalsIgnoreCase(s.getDbType()));
        XaLog xaLog = MetaClusterCurrent.wrapper(XaLog.class);

        if (allMatchMySQL) {
            Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
            boolean hasXA = authenticator.getConfigAsList().stream().anyMatch(u -> TransactionType.parse(u.getTransactionType()) == TransactionType.JDBC_TRANSACTION_TYPE);
            if (hasXA) {
                LOGGER.info("readXARecoveryLog start");
                xaLog.readXARecoveryLog();
            }
        }
    }

    private Resource<ReplicaSelectorManager> getReplicaSelectorManager(UpdateSet<ClusterConfig> clusterConfigUpdateSet,
                                                                       UpdateSet<DatasourceConfig> datasourceConfigUpdateSet,
                                                                       Resource<JdbcConnectionManager> jdbcConnectionManagerResource) {
        boolean hasChanged = !clusterConfigUpdateSet.isEmpty() || !datasourceConfigUpdateSet.isEmpty();
        if (!hasChanged && MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(ReplicaSelectorManager.class), true);
        }
        LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
        Map<String, DatasourceConfig> datasourceConfigMap =
                datasourceConfigUpdateSet.getTarget().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        ArrayList<ClusterConfig> clusterConfigs = new ArrayList<>(clusterConfigUpdateSet.getTarget());
        ReplicaSelectorManager replicaSelector = new ReplicaSelectorRuntime(clusterConfigs, datasourceConfigMap, loadBalanceManager,
                name -> {
                    try {
                        MySQLManager manager = MetaClusterCurrent.wrapper(MySQLManager.class);
                        return manager.getSessionCount(name);
                    } catch (Exception e) {
                        LOGGER.error("", e);
                        return 0;
                    }
                }, (command, initialDelay, period, unit) -> {
            ScheduledFuture<?> scheduled = ScheduleUtil.getTimer().scheduleAtFixedRate(command, initialDelay, period, unit);
            return () -> {
                try {
                    if (scheduled != null && (!scheduled.isDone() || !scheduled.isCancelled())) {
                        scheduled.cancel(true);
                    }
                } catch (Throwable throwable) {
                    LOGGER.error("", throwable);
                }
            };
        });
        JdbcConnectionManager jdbcConnectionManager = jdbcConnectionManagerResource.get();
        replicaSelector = new MonitorReplicaSelectorManager(replicaSelector);
        jdbcConnectionManager.register(replicaSelector);
        if (!replicaSelector.getConfig().equals(clusterConfigUpdateSet.getTargetAsList())) {
            throw new UnsupportedOperationException();
        }
        return Resource.of(replicaSelector, false);
    }

    @NotNull
    private Resource<DatasourceConfigProvider> getDatasourceConfigProvider(UpdateSet<DatasourceConfig> datasourceConfigUpdateSet) {
        if (datasourceConfigUpdateSet.isEmpty() && MetaClusterCurrent.exist(DatasourceConfigProvider.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(DatasourceConfigProvider.class), true);
        }
        return Resource.of(new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return datasourceConfigUpdateSet.getTargetAsMap();
            }
        }, false);
    }

    @NotNull
    private Resource<JdbcConnectionManager> getJdbcConnectionManager(UpdateSet<DatasourceConfig> datasourceConfigUpdateSet) {
        if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            if (!datasourceConfigUpdateSet.isEmpty()) {
                if (datasourceConfigUpdateSet.getDelete().isEmpty() && !datasourceConfigUpdateSet.getCreate().isEmpty()) {
                    for (DatasourceConfig datasourceConfig : datasourceConfigUpdateSet.getCreate()) {
                        jdbcConnectionManager.addDatasource(datasourceConfig);
                    }
                    return Resource.of(jdbcConnectionManager, true);
                } else {
                    jdbcConnectionManager.close();
                    return Resource.of(new JdbcConnectionManager(
                            DruidDatasourceProvider.class.getCanonicalName(),
                            datasourceConfigUpdateSet.getTargetAsMap()), false);
                }
            } else {
                return Resource.of(jdbcConnectionManager, true);
            }
        } else {
            return Resource.of(new JdbcConnectionManager(
                    DruidDatasourceProvider.class.getCanonicalName(),
                    datasourceConfigUpdateSet.getTargetAsMap()), false);
        }
    }

    @NotNull
    private Resource<SequenceGenerator> getSequenceGenerator(UpdateSet<SequenceConfig> sequenceConfigUpdateSet) {
        if (sequenceConfigUpdateSet.isEmpty() && MetaClusterCurrent.exist(SequenceGenerator.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(SequenceGenerator.class), true);
        }
        Collection<SequenceConfig> target = sequenceConfigUpdateSet.getTarget();
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        SequenceGenerator sequenceGenerator = new SequenceGenerator(serverConfig.getMycatId(), target);
        return Resource.of(sequenceGenerator, false);
    }

    @NotNull
    private Resource<SqlResultSetService> getSqlResultSetService(UpdateSet<SqlCacheConfig> sqlCacheConfigUpdateSet) {
        if (sqlCacheConfigUpdateSet.isEmpty() && MetaClusterCurrent.exist(SqlResultSetService.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(SqlResultSetService.class), true);
        }
        SqlResultSetService sqlResultSetService2;
        SqlResultSetService sqlResultSetService = new SqlResultSetService();
        for (SqlCacheConfig sqlCacheConfig : sqlCacheConfigUpdateSet.getTarget()) {
            sqlResultSetService.addIfNotPresent(sqlCacheConfig);
        }
        sqlResultSetService2 = sqlResultSetService;
        return Resource.of(sqlResultSetService2, false);
    }

    @NotNull
    private Resource<Authenticator> getAuthenticator(UpdateSet<UserConfig> userConfigUpdateSet) {
        if (userConfigUpdateSet.isEmpty() && MetaClusterCurrent.exist(Authenticator.class)) {
            return Resource.of(MetaClusterCurrent.wrapper(Authenticator.class), true);
        }
        return Resource.of(new AuthenticatorImpl(userConfigUpdateSet.getTargetAsMap()), false);
    }

    public void persistence(UpdateSet<LogicSchemaConfig> schemaConfigUpdateSet,
                            UpdateSet<ClusterConfig> clusterConfigUpdateSet,
                            UpdateSet<DatasourceConfig> datasourceConfigUpdateSet, UpdateSet<SequenceConfig> sequenceConfigUpdateSet, UpdateSet<SqlCacheConfig> sqlCacheConfigUpdateSet, UpdateSet<UserConfig> userConfigUpdateSet) {
        List<KV<LogicSchemaConfig>> schemaKvs = Arrays.asList(storageManager.get(LogicSchemaConfig.class));
        List<KV<ClusterConfig>> clusterKvs = Arrays.asList(storageManager.get(ClusterConfig.class));
        List<KV<DatasourceConfig>> datasourceKvs = Arrays.asList(storageManager.get(DatasourceConfig.class));
        List<KV<SequenceConfig>> sequenceKvs = Arrays.asList(storageManager.get(SequenceConfig.class));
        List<KV<SqlCacheConfig>> sqlCacheKvs = Arrays.asList(storageManager.get(SqlCacheConfig.class));
        List<KV<UserConfig>> userKvs = Arrays.asList(storageManager.get(UserConfig.class));


        schemaKvs.forEach(kv1 -> schemaConfigUpdateSet.execute(kv1));
        clusterKvs.forEach(kv1 -> clusterConfigUpdateSet.execute(kv1));
        datasourceKvs.forEach(kv1 -> datasourceConfigUpdateSet.execute(kv1));
        sequenceKvs.forEach(kv1 -> sequenceConfigUpdateSet.execute(kv1));
        sqlCacheKvs.forEach(kv1 -> sqlCacheConfigUpdateSet.execute(kv1));
        userKvs.forEach(kv1 -> userConfigUpdateSet.execute(kv1));
    }

    @Override
    public void reset() {
        MycatRouterConfig newMycatRouterConfig = new MycatRouterConfig();
        defaultConfig(newMycatRouterConfig);
        this.newConfig = newMycatRouterConfig;
    }

    public static void defaultConfig(MycatRouterConfig routerConfig) {
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
                clusterConfig.setMasters(Collections.singletonList("prototypeDs"));
                clusterConfig.setMaxCon(200);
                clusterConfig.setClusterType(ReplicaType.MASTER_SLAVE.name());
                clusterConfig.setSwitchType(ReplicaSwitchType.SWITCH.name());
                routerConfig.getClusters().add(clusterConfig);
            }
        }
        routerConfig.fixPrototypeTargetName();
        routerConfig.setSchemas(
                new ArrayList<>(
                        fix(routerConfig.getSchemas().stream().collect(Collectors.toMap(k -> k.getSchemaName(), v -> v))).values()
                )
        );
    }

    @Override
    public void close() {

    }


    public static Map<String, Object> getAutoHashProperties(MySqlCreateTableStatement createTableSql) {
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
        return properties;
    }

    private static Map<String, LogicSchemaConfig> fix(Map<String, LogicSchemaConfig> orginal) {
        orginal = new HashMap<>(orginal);
        Set<String> databases = new HashSet<>();
        databases.add("information_schema");
        databases.add("mysql");
        databases.add("performance_schema");


        for (String database : databases) {
            if (!orginal.containsKey(database)) {
                LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
                schemaConfig.setSchemaName(database);
                schemaConfig.setTargetName(MetadataManager.getPrototype());
                orginal.put(database, schemaConfig);
            }
        }

        ArrayList<LogicSchemaConfig> logicSchemaConfigs = new ArrayList<>();
        addInnerTable(logicSchemaConfigs);
        for (LogicSchemaConfig logicSchemaConfig : logicSchemaConfigs) {
            if (!orginal.containsKey(logicSchemaConfig.getSchemaName())) {
                orginal.put(logicSchemaConfig.getSchemaName(), logicSchemaConfig);
            }
        }
        return orginal;
    }

    private static void addInnerTable(List<LogicSchemaConfig> schemaConfigs) {
        String schemaName = "mysql";
        String targetName = MetadataManager.getPrototype();
        String tableName = "proc";

        LogicSchemaConfig logicSchemaConfig = schemaConfigs.stream()
                .filter(i -> schemaName.equalsIgnoreCase(i.getSchemaName()))
                .findFirst()
                .orElseGet(() -> {
                    LogicSchemaConfig config = new LogicSchemaConfig();
                    config.setSchemaName(schemaName);
                    config.setTargetName(MetadataManager.prototype);
                    schemaConfigs.add(config);
                    return config;
                });


        Map<String, NormalTableConfig> normalTables = logicSchemaConfig.getNormalTables();
        normalTables.putIfAbsent(tableName, NormalTableConfig.create(schemaName, tableName,
                "CREATE TABLE `mysql`.`proc` (\n" +
                        "  `db` varchar(64) DEFAULT NULL,\n" +
                        "  `name` varchar(64) DEFAULT NULL,\n" +
                        "  `type` enum('FUNCTION','PROCEDURE','PACKAGE', 'PACKAGE BODY'),\n" +
                        "  `specific_name` varchar(64) DEFAULT NULL,\n" +
                        "  `language` enum('SQL'),\n" +
                        "  `sql_data_access` enum('CONTAINS_SQL', 'NO_SQL', 'READS_SQL_DATA', 'MODIFIES_SQL_DATA'),\n" +
                        "  `is_deterministic` enum('YES','NO'),\n" +
                        "  `security_type` enum('INVOKER','DEFINER'),\n" +
                        "  `param_list` blob,\n" +
                        "  `returns` longblob,\n" +
                        "  `body` longblob,\n" +
                        "  `definer` varchar(141),\n" +
                        "  `created` timestamp,\n" +
                        "  `modified` timestamp,\n" +
                        "  `sql_mode` \tset('REAL_AS_FLOAT', 'PIPES_AS_CONCAT', 'ANSI_QUOTES', 'IGNORE_SPACE', 'IGNORE_BAD_TABLE_OPTIONS', 'ONLY_FULL_GROUP_BY', 'NO_UNSIGNED_SUBTRACTION', 'NO_DIR_IN_CREATE', 'POSTGRESQL', 'ORACLE', 'MSSQL', 'DB2', 'MAXDB', 'NO_KEY_OPTIONS', 'NO_TABLE_OPTIONS', 'NO_FIELD_OPTIONS', 'MYSQL323', 'MYSQL40', 'ANSI', 'NO_AUTO_VALUE_ON_ZERO', 'NO_BACKSLASH_ESCAPES', 'STRICT_TRANS_TABLES', 'STRICT_ALL_TABLES', 'NO_ZERO_IN_DATE', 'NO_ZERO_DATE', 'INVALID_DATES', 'ERROR_FOR_DIVISION_BY_ZERO', 'TRADITIONAL', 'NO_AUTO_CREATE_USER', 'HIGH_NOT_PRECEDENCE', 'NO_ENGINE_SUBSTITUTION', 'PAD_CHAR_TO_FULL_LENGTH', 'EMPTY_STRING_IS_NULL', 'SIMULTANEOUS_ASSIGNMENT'),\n" +
                        "  `comment` text,\n" +
                        "  `character_set_client` char(32),\n" +
                        "  `collation_connection` \tchar(32),\n" +
                        "  `db_collation` \tchar(32),\n" +
                        "  `body_utf8` \tlongblob,\n" +
                        "  `aggregate` \tenum('NONE', 'GROUP')\n" +
                        ") ", targetName));

        LogicSchemaConfig mycat = schemaConfigs.stream().filter(i ->
                        "mycat".equalsIgnoreCase(i.getSchemaName()))
                .findFirst().orElseGet(() -> {
                    LogicSchemaConfig schemaConfig = new LogicSchemaConfig();
                    schemaConfig.setSchemaName("mycat");
                    schemaConfigs.add(schemaConfig);
                    return schemaConfig;
                });
        Map<String, CustomTableConfig> customTables = mycat.getCustomTables();

        customTables.computeIfAbsent("dual", (n) -> {
            CustomTableConfig tableConfig = CustomTableConfig.builder().build();
            tableConfig.setClazz(DualCustomTableHandler.class.getCanonicalName());
            tableConfig.setCreateTableSQL("create table mycat.dual(id int)");
            return tableConfig;
        });
    }

    public void addProcedure(String schemaName, String pName, NormalProcedureConfig normalProcedureConfig) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> schemaName.equalsIgnoreCase(i.getSchemaName())).findFirst();
        if (first.isPresent()) {
            LogicSchemaConfig logicSchemaConfig = first.get();
            Map<String, NormalProcedureConfig> normalProcedures = logicSchemaConfig.getNormalProcedures();
            normalProcedures.put(pName,normalProcedureConfig);
        }
    }

    public void removeProcedure(String schemaName, String pName) {
        List<LogicSchemaConfig> schemas = newConfig.getSchemas();
        Optional<LogicSchemaConfig> first = schemas.stream().filter(i -> schemaName.equalsIgnoreCase(i.getSchemaName())).findFirst();
        if (first.isPresent()) {
            LogicSchemaConfig logicSchemaConfig = first.get();
            Map<String, NormalProcedureConfig> normalProcedures = logicSchemaConfig.getNormalProcedures();
            normalProcedures.remove(pName);
        }
    }
}
