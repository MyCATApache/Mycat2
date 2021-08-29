package io.mycat.config;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.impl.LocalXaMemoryRepositoryImpl;
import cn.mycat.vertx.xa.impl.XaLogImpl;
import io.mycat.*;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.calcite.spm.DbPlanManagerPersistorImpl;
import io.mycat.calcite.spm.MemPlanCache;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.spm.UpdatePlanCache;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.commands.MycatMySQLManagerImpl;
import io.mycat.commands.SqlResultSetService;
import io.mycat.config.controller.*;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.monitor.MonitorReplicaSelectorManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.statistic.StatisticCenter;
import io.mycat.util.NameMap;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

public class CoreMetadataStorageManager implements BaseMetadataStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoreMetadataStorageManager.class);

    public CoreMetadataStorageManager(MycatServerConfig mycatServerConfig) {
        ServerConfig serverConfig = mycatServerConfig.getServer();

        MetaClusterCurrent.register(MycatServerConfig.class, mycatServerConfig);
        MetaClusterCurrent.register(ServerConfig.class, mycatServerConfig.getServer());
        StatisticCenter statisticCenter = new StatisticCenter();
        MetaClusterCurrent.register(StatisticCenter.class, statisticCenter);
        LocalXaMemoryRepositoryImpl localXaMemoryRepository = LocalXaMemoryRepositoryImpl.createLocalXaMemoryRepository(() -> MetaClusterCurrent.wrapper(MySQLManager.class));
        MetaClusterCurrent.register(XaLog.class, new XaLogImpl(localXaMemoryRepository, serverConfig.getMycatId(),() -> MetaClusterCurrent.wrapper(MySQLManager.class)));
        if (!MetaClusterCurrent.exist(LoadBalanceManager.class)) {
            MetaClusterCurrent.register(LoadBalanceManager.class, new LoadBalanceManager(mycatServerConfig.getLoadBalance()));
        }
        List<ClusterConfig> clusterConfigs = Collections.emptyList();
        Map<String, DatasourceConfig> datasourceConfigMap = Collections.emptyMap();
        ClusterController.update(clusterConfigs, datasourceConfigMap);
        JdbcManagerController.update(datasourceConfigMap);
        SequenceController.update(Collections.emptyList());
        AuthenticatorController.update(Collections.emptyMap());
        UserController.update(Collections.emptyMap());
        SchemaController.update(Collections.emptyMap());
    }


    @Override
    public void putSchema(LogicSchemaConfig schemaConfig) {
        SchemaController.addSchema(schemaConfig);
    }

    @Override
    public void dropSchema(String schemaName) {
        SchemaController.removeSchema(schemaName);
    }

    @Override
    public void putTable(CreateTableConfig createTableConfig) {
        SchemaController.addTable(createTableConfig);
    }


    @Override
    public void removeTable(String schemaNameArg, String tableNameArg) {
        SchemaController.removeTable(schemaNameArg,tableNameArg);
    }


    @Override
    public void putUser(UserConfig userConfig) {
        UserController.add(userConfig);
    }

    @Override
    public void deleteUser(String username) {
        UserController.remove(username);
    }

    @Override
    public void putSequence(SequenceConfig sequenceConfig) {
        SequenceController.add(sequenceConfig);
    }

    @Override
    public void removeSequenceByName(String name) {
        SequenceController.remove(name);
    }

    @Override
    public void putDatasource(DatasourceConfig datasourceConfig) {
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        connectionManager.addDatasource(datasourceConfig);

        ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        connectionManager.registerReplicaSelector(replicaSelectorManager);
        MetaClusterCurrent.register(DatasourceConfigProvider.class, new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return connectionManager.getConfig();
            }
        });
        MetaClusterCurrent.register(MySQLManager.class, new MycatMySQLManagerImpl(new ArrayList<>(connectionManager.getConfig().values())));
    }

    @Override
    public void removeDatasource(String datasourceName) {
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        connectionManager.removeDatasource(datasourceName);

        ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        connectionManager.registerReplicaSelector(replicaSelectorManager);

        MetaClusterCurrent.register(DatasourceConfigProvider.class, new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return connectionManager.getConfig();
            }
        });

        MetaClusterCurrent.register(MySQLManager.class, new MycatMySQLManagerImpl(new ArrayList<>(connectionManager.getConfig().values())));
    }

    @Override
    @SneakyThrows
    public void putReplica(ClusterConfig clusterConfig) {
        ClusterController.addCluster(clusterConfig);
    }

    @NotNull
    public static ReplicaSelectorManager createReplicaSelector(List<ClusterConfig> clusterConfigs, LoadBalanceManager loadBalanceManager, Map<String, DatasourceConfig> datasourceConfigMap) {
        ReplicaSelectorRuntime selectorRuntime = new ReplicaSelectorRuntime(clusterConfigs, datasourceConfigMap, loadBalanceManager,
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
        return new MonitorReplicaSelectorManager(selectorRuntime);
    }

    @Override
    public void removeReplica(String replicaName) {
        ClusterController.removeCluster(replicaName);
    }

    @Override
    public void sync(MycatRouterConfig mycatRouterConfig) {

        BaseMetadataStorageManager.defaultConfig(mycatRouterConfig);

        Map<String, LogicSchemaConfig> schemaMap = mycatRouterConfig.getSchemas().stream().collect(Collectors.toMap(k -> k.getSchemaName(), v -> v));
        List<ClusterConfig> clusterList = mycatRouterConfig.getClusters();
        Map<String, ClusterConfig> clusterMap = clusterList.stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        List<DatasourceConfig> datasources = mycatRouterConfig.getDatasources();
        List<UserConfig> users = mycatRouterConfig.getUsers();
        List<SequenceConfig> sequences = mycatRouterConfig.getSequences();
        List<SqlCacheConfig> sqlCacheConfigs = mycatRouterConfig.getSqlCacheConfigs();

        mycatRouterConfig.getSchemas().forEach(s -> putSchema(s));

        mycatRouterConfig.getDatasources().forEach(ds -> putDatasource(ds));
        mycatRouterConfig.getClusters().forEach(c -> putReplica(c));
        mycatRouterConfig.getUsers().forEach(userConfig -> putUser(userConfig));
        mycatRouterConfig.getSequences().forEach(s -> putSequence(s));
        mycatRouterConfig.getSqlCacheConfigs().forEach(c -> putSqlCache(c));


        MetaClusterCurrent.register(MysqlVariableService.class,new MysqlVariableServiceImpl(MetaClusterCurrent.wrapper(JdbcConnectionManager.class)));
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        ioExecutor.executeBlocking((Handler<Promise<Void>>) promise -> {
            try {
                if (MetaClusterCurrent.exist(MemPlanCache.class)){
                    MemPlanCache memPlanCache = MetaClusterCurrent.wrapper(MemPlanCache.class);
                    memPlanCache.init();
                }
            }finally {
                promise.tryComplete();
            }
        });
        boolean allMatchMySQL = datasources.stream().allMatch(s -> "mysql".equalsIgnoreCase(s.getDbType()));
        XaLog xaLog = MetaClusterCurrent.wrapper(XaLog.class);
        if (allMatchMySQL) {
            Authenticator  authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
            boolean hasXA = authenticator.allUsers().stream().anyMatch(u -> TransactionType.parse(u.getTransactionType()) == TransactionType.JDBC_TRANSACTION_TYPE);
            if (hasXA) {
                LOGGER.info("readXARecoveryLog start");
                xaLog.readXARecoveryLog();
            }
        }
    }

    @Override
    public void putSqlCache(SqlCacheConfig sqlCacheConfig) {
        SqlCacheContoller.add(sqlCacheConfig);
    }

    @Override
    public void removeSqlCache(String name) {
        SqlCacheContoller.remove(name);
    }

    @Override
    public MycatRouterConfig getConfig() {
        MycatRouterConfig mycatRouterConfig = new MycatRouterConfig();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        ArrayList<LogicSchemaConfig> schemaConfigs = new ArrayList<>();
        for (Map.Entry<String, SchemaHandler> e : metadataManager.getSchemaMap().entrySet()) {
            SchemaHandler schemaHandler = e.getValue();
            LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
            logicSchemaConfig.setSchemaName(e.getKey());
            logicSchemaConfig.setTargetName(schemaHandler.defaultTargetName());
            schemaConfigs.add(logicSchemaConfig);


            for (TableHandler tableHandler : schemaHandler.logicTables().values()) {
                switch (tableHandler.getType()) {
                    case SHARDING: {
                        ShardingTable shardingTable = (ShardingTable) tableHandler;
                        ShardingTableConfig tableConfig = shardingTable.getTableConfig();
                        if (tableConfig == null) continue;
                        logicSchemaConfig.getShardingTables().put(shardingTable.getSchemaName(), tableConfig);
                        break;
                    }
                    case GLOBAL: {
                        GlobalTable globalTable = (GlobalTable) tableHandler;
                        logicSchemaConfig.getGlobalTables().put(globalTable.getSchemaName(), globalTable.getTableConfig());
                        break;
                    }
                    case NORMAL: {
                        NormalTable normalTable = (NormalTable) tableHandler;
                        logicSchemaConfig.getNormalTables().put(normalTable.getSchemaName(), normalTable.getTableConfig());
                        break;

                    }
                    case CUSTOM:
                        break;
                }
            }


        }
        mycatRouterConfig.setSchemas(schemaConfigs);


        ReplicaSelectorManager replicaSelectorManager = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        mycatRouterConfig.setClusters(replicaSelectorManager.getConfig());


        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        mycatRouterConfig.setDatasources(new ArrayList<>(jdbcConnectionManager.getConfig().values()));

        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        mycatRouterConfig.setUsers(authenticator.allUsers());

        SequenceGenerator sequenceGenerator = MetaClusterCurrent.wrapper(SequenceGenerator.class);
        mycatRouterConfig.setSequences(sequenceGenerator.getSequencesConfig());

        return mycatRouterConfig;
    }

    @Override
    public void reportReplica(Map<String, List<String>> dsNames) {

    }
}
