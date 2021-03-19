package io.mycat.config;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.SimpleConfig;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.impl.LocalXaMemoryRepositoryImpl;
import cn.mycat.vertx.xa.impl.XaLogImpl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import io.mycat.*;
import io.mycat.calcite.spm.PlanCache;
import io.mycat.commands.MycatMySQLManagerImpl;
import io.mycat.commands.SqlResultSetService;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.replica.*;
import io.vertx.core.Future;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConfigPrepareExecuter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPrepareExecuter.class);
    private final MycatRouterConfigOps ops;
    ////////////////////////////////////////////////////////////////////////////////////
    private ReplicaSelectorManager replicaSelector;
    private JdbcConnectionManager jdbcConnectionManager;
    private MetadataManager metadataManager;
    private DatasourceConfigProvider datasourceConfigProvider;
    private Authenticator authenticator;
    private MetadataStorageManager metadataStorageManager;
    private SequenceGenerator sequenceGenerator;


    private String datasourceProvider;
    private SqlResultSetService sqlResultSetService;


//    UpdateType updateType = UpdateType.FULL;


    //originalRouterConfig = YamlUtil.load(MycatRouterConfig.class, new FileReader(baseDirectory.resolve("metadata.yml").toFile()));
    public ConfigPrepareExecuter(
            MycatRouterConfigOps ops,
            MetadataStorageManager metadataStorageManager,
            String datasourceProvider) {
        this.ops = ops;
        this.metadataStorageManager = metadataStorageManager;
        this.datasourceProvider = datasourceProvider;

        boolean router = ops.getSchemas() != null;
        boolean cluster = ops.getClusters() != null;
        boolean users = ops.getUsers() != null;
        boolean sequences = ops.getSequences() != null;
        boolean datasources = ops.getDatasources() != null;

//
//        if (router && !cluster && !users && !sequences && !datasources) {
//            updateType = UpdateType.ROUTER;
//        } else if (!router && !cluster && users && !sequences && !datasources) {
//            updateType = UpdateType.USER;
//        } else if (!router && !cluster && !users && sequences && !datasources) {
//            updateType = UpdateType.SEQUENCE;
//        } else {
//            updateType = UpdateType.FULL;
//        }

    }

    public void prepareRuntimeObject() {

        switch (ops.getUpdateType()) {
            case USER: {
                LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
                MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);

                this.authenticator = new AuthenticatorImpl(ops.getUsers().stream().distinct().collect(Collectors.toMap(k -> k.getUsername(), v -> v)));
                break;
            }
            case SEQUENCE: {
                LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
                MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                ServerConfig serverConfig = MetaClusterCurrent.wrapper(MycatServerConfig.class).getServer();
                this.sequenceGenerator = new SequenceGenerator(serverConfig.getMycatId(), ops.getSequences());
                break;
            }
            case ROUTER: {
                this.metadataManager = createMetaData();
                clearSqlCache();
                break;
            }
            case CREATE_TABLE: {
                String schemaName = ops.getSchemaName();
                String tableName = ops.getTableName();
                this.metadataManager = createMetaData();
                TableHandler table = this.metadataManager.getTable(schemaName, tableName);
                table.createPhysicalTables();
                clearSqlCache();
                break;
            }
            case DROP_TABLE: {
                MetadataManager oldMetadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

                String schemaName = ops.getSchemaName();
                String tableName = ops.getTableName();
                this.metadataManager = createMetaData();

                TableHandler table = oldMetadataManager.getTable(schemaName, tableName);
                if (table != null) {
                    table.dropPhysicalTables();
                }
                clearSqlCache();
                break;
            }
            case FULL: {
                MycatRouterConfig mycatRouterConfig = ops.getMycatRouterConfig();
                fullInitBy(mycatRouterConfig);
                clearSqlCache();
                break;
            }

            case CREATE_SQL_CACHE: {
                if (MetaClusterCurrent.exist(SqlResultSetService.class)) {
                    SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
                    if (sqlResultSetService != null) {
                        sqlResultSetService.addIfNotPresent(ops.sqlCache);
                    }
                }
                break;
            }
            case DROP_SQL_CACHE: {
                if (MetaClusterCurrent.exist(SqlResultSetService.class)) {
                    SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
                    if (sqlResultSetService != null) {
                        sqlResultSetService.dropByName(ops.sqlCache.getName());
                    }
                }
                break;
            }
            case RESET:
                MycatRouterConfig routerConfig = new MycatRouterConfig();
                FileMetadataStorageManager.defaultConfig(routerConfig);
                fullInitBy(routerConfig);
                break;
        }
    }

    @NotNull
    private MetadataManager createMetaData() {
        return MetadataManager.createMetadataManager(ops.getSchemas(),
                MetaClusterCurrent.wrapper(LoadBalanceManager.class),
                MetaClusterCurrent.wrapper(SequenceGenerator.class),
                MetaClusterCurrent.wrapper(ReplicaSelectorManager.class),
                MetaClusterCurrent.wrapper(JdbcConnectionManager.class),
                "prototype");
    }

    public void fullInitBy(MycatRouterConfig mycatRouterConfig) {

        LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        Map<String, DatasourceConfig> datasourceConfigMap = mycatRouterConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        Map<String, ClusterConfig> clusters = mycatRouterConfig.getClusters().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        replicaSelector = new ReplicaSelectorRuntime(mycatRouterConfig.getClusters(), datasourceConfigMap, loadBalanceManager,
                name -> {
                    MySQLManager manager = MetaClusterCurrent.wrapper(MySQLManager.class);
                    return manager.getSessionCount(name);
                }, (command, initialDelay, period, unit) -> {
                    ScheduledFuture<?> scheduled = ScheduleUtil.getTimer().scheduleAtFixedRate(command, initialDelay, period, unit);
                    return () -> {
                        try {
                            if (scheduled != null && (!scheduled.isDone() || !scheduled.isCancelled())) {
                                scheduled.cancel(true);
                            }
                        }catch (Throwable throwable){
                            LOGGER.error("",throwable);
                        }
                    };
                });
        jdbcConnectionManager = new JdbcConnectionManager(
                datasourceProvider,
                datasourceConfigMap,
                clusters,
                mycatWorkerProcessor,
                replicaSelector);
        datasourceConfigProvider = new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return datasourceConfigMap;
            }
        };
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(MycatServerConfig.class).getServer();
        this.sequenceGenerator = new SequenceGenerator(serverConfig.getMycatId(), mycatRouterConfig.getSequences());
        this.authenticator = new AuthenticatorImpl(mycatRouterConfig.getUsers().stream().collect(Collectors.toMap(k -> k.getUsername(), v -> v)));
        this.metadataManager = MetadataManager.createMetadataManager(mycatRouterConfig.getSchemas(), loadBalanceManager, sequenceGenerator, replicaSelector, jdbcConnectionManager, mycatRouterConfig.getPrototype());

        if (MetaClusterCurrent.exist(SqlResultSetService.class)) {
            SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
            sqlResultSetService.close();
        }
        this.sqlResultSetService = new SqlResultSetService();
        for (SqlCacheConfig sqlCacheConfig : mycatRouterConfig.getSqlCacheConfigs()) {
            this.sqlResultSetService.addIfNotPresent(sqlCacheConfig);
        }


        ////////////////////////////////////////////////////////
    }

    private void clearSqlCache() {
        if (MetaClusterCurrent.exist(SqlResultSetService.class)) {
            this.sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
            if (sqlResultSetService != null) {
                sqlResultSetService.clear();
            }
        }
    }

    public void prepareStoreDDL() {

    }

    public enum UpdateType {
        USER,
        SEQUENCE,
        ROUTER,
        CREATE_TABLE,
        DROP_TABLE,
        FULL
    }


    public ReplicaSelectorManager getReplicaSelector() {
        return replicaSelector;
    }

    public JdbcConnectionManager getJdbcConnectionManager() {
        return jdbcConnectionManager;
    }

    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    public DatasourceConfigProvider getDatasourceConfigProvider() {
        return datasourceConfigProvider;
    }

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public Future<Void> commit() throws IOException {

        ReplicaSelectorManager replicaSelector = this.replicaSelector;
        JdbcConnectionManager jdbcConnectionManager = this.jdbcConnectionManager;
        MetadataManager metadataManager = this.metadataManager;
        DatasourceConfigProvider datasourceConfigProvider = this.datasourceConfigProvider;
        Authenticator authenticator = this.authenticator;
        MetadataStorageManager metadataStorageManager = this.metadataStorageManager;
        SequenceGenerator sequenceGenerator = this.sequenceGenerator;
        SqlResultSetService sqlResultSetService = this.sqlResultSetService;
        MycatRouterConfig mycatRouterConfig = ops.getMycatRouterConfig();

        HashMap<Class, Object> context = new HashMap<>();

        Map<Class, Object> oldContext = MetaClusterCurrent.context.get();

        if (oldContext != null) {
            context.putAll(oldContext);
        }

        if (replicaSelector != null) {
            if (MetaClusterCurrent.exist(ReplicaSelectorManager.class)) {
                ReplicaSelectorManager replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
                replicaSelectorRuntime.close();
            }
            context.put(ReplicaSelectorManager.class, replicaSelector);
        }
        if (jdbcConnectionManager != null) {
            if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
                JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                connectionManager.close();
            }
            context.put(JdbcConnectionManager.class, jdbcConnectionManager);
        }
        if (metadataManager != null) {
            context.put(metadataManager.getClass(), metadataManager);
            context.put(MysqlVariableService.class, metadataManager);
        }
        if (datasourceConfigProvider != null) {
            context.put(datasourceConfigProvider.getClass(), datasourceConfigProvider);
            context.put(DatasourceConfigProvider.class, datasourceConfigProvider);
        }
        if (authenticator != null) {
            context.put(Authenticator.class, authenticator);
            context.put(authenticator.getClass(), authenticator);
        }
        if (metadataStorageManager != null) {
            context.put(metadataStorageManager.getClass(), metadataStorageManager);
            context.put(MetadataStorageManager.class, metadataStorageManager);
            context.put(ReplicaReporter.class, metadataStorageManager);
        }
        if (sequenceGenerator != null) {
            context.put(sequenceGenerator.getClass(), sequenceGenerator);
        }
        if (mycatRouterConfig != null) {
            context.put(MycatRouterConfig.class, mycatRouterConfig);
        }
        if (sqlResultSetService != null) {
            context.put(SqlResultSetService.class, sqlResultSetService);
        }
        PlanCache.INSTANCE.clear();

        MySQLManager mySQLManager;
        context.put(MySQLManager.class, mySQLManager = new MycatMySQLManagerImpl((MycatRouterConfig) context.get(MycatRouterConfig.class)));

        context.put(DrdsRunner.class, new DrdsRunner(() -> ((MetadataManager) context.get(MetadataManager.class)).getSchemaMap(), PlanCache.INSTANCE));
        ServerConfig serverConfig = (ServerConfig) context.get(ServerConfig.class);
        LocalXaMemoryRepositoryImpl localXaMemoryRepository = LocalXaMemoryRepositoryImpl.createLocalXaMemoryRepository(() -> mySQLManager);
        context.put(XaLog.class, new XaLogImpl(localXaMemoryRepository, serverConfig.getMycatId(), Objects.requireNonNull(mySQLManager)));
        MetaClusterCurrent.register(context);

        MycatRouterConfig curConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        boolean allMatchMySQL = curConfig.getDatasources().stream().allMatch(s -> "mysql".equalsIgnoreCase(s.getDbType()));
        if (allMatchMySQL){
            XaLog xaLog = MetaClusterCurrent.wrapper(XaLog.class);
            return xaLog.readXARecoveryLog();
        }else {
            return Future.succeededFuture();
        }
    }
}