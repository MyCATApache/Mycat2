package io.mycat.config;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.SimpleConfig;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.impl.MySQLManagerImpl;
import cn.mycat.vertx.xa.impl.XaLogImpl;
import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import io.mycat.*;
import io.mycat.calcite.spm.PlanCache;
import io.mycat.commands.SqlResultSetService;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConfigPrepareExecuter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPrepareExecuter.class);
    private final MycatRouterConfigOps ops;
    ////////////////////////////////////////////////////////////////////////////////////
    private ReplicaSelectorRuntime replicaSelector;
    private JdbcConnectionManager jdbcConnectionManager;
    private MetadataManager metadataManager;
    private DatasourceConfigProvider datasourceConfigProvider;
    private Authenticator authenticator;
    private MetadataStorageManager metadataStorageManager;
    private SequenceGenerator sequenceGenerator;


    private String datasourceProvider;
    private SqlResultSetService sqlResultSetService;
    private MySQLManager mySQLManager;
    private XaLog xaLog;
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
                MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class),
                MetaClusterCurrent.wrapper(JdbcConnectionManager.class),
                "prototype");
    }

    public void fullInitBy(MycatRouterConfig mycatRouterConfig) {

        LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        Map<String, DatasourceConfig> datasourceConfigMap = mycatRouterConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        Map<String, ClusterConfig> clusters = mycatRouterConfig.getClusters().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        replicaSelector = new ReplicaSelectorRuntime(mycatRouterConfig.getClusters(), datasourceConfigMap, loadBalanceManager, metadataStorageManager);
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

        List<SimpleConfig> configList = new ArrayList<>();
        if (serverConfig.isProxy()) {
            for (DatasourceConfig datasource : mycatRouterConfig.getDatasources()) {
                if (!"mysql".equalsIgnoreCase(datasource.getDbType())) {
                    throw new IllegalArgumentException(datasource.toString() + "  \n is not mysql type");
                }
                ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(datasource.getUrl());
                HostInfo hostInfo = connectionUrlParser.getHosts().get(0);
                String name = datasource.getName();
                String host = hostInfo.getHost();
                int port = hostInfo.getPort();
                String user = Optional.ofNullable(datasource.getUser()).orElse(hostInfo.getUser());
                String password = Optional.ofNullable(datasource.getPassword()).orElse(hostInfo.getPassword());
                String database = hostInfo.getDatabase();
                int maxSize = datasource.getMaxCon();
                SimpleConfig simpleConfig = new SimpleConfig(name, host, port, user, password, database, maxSize);
                configList.add(simpleConfig);
            }
        }
        this.mySQLManager = new MySQLManagerImpl(configList);
        this.xaLog = XaLogImpl.createDemoRepository(mySQLManager);
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


    public ReplicaSelectorRuntime getReplicaSelector() {
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

    public void commit() {

        ReplicaSelectorRuntime replicaSelector = this.replicaSelector;
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
            if (MetaClusterCurrent.exist(ReplicaSelectorRuntime.class)) {
                ReplicaSelectorRuntime replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
                replicaSelectorRuntime.close();
            }
            context.put(ReplicaSelectorRuntime.class, replicaSelector);
        }
        if (jdbcConnectionManager != null) {
            if (MetaClusterCurrent.exist(JdbcConnectionManager.class)) {
                JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
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
        if (xaLog != null) {
            context.put(XaLog.class, xaLog);
        }
        if (mySQLManager != null) {

            if (MetaClusterCurrent.exist(MySQLManager.class)) {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                MySQLManager mySQLManager = MetaClusterCurrent.wrapper(MySQLManager.class);
                mySQLManager.close(event -> {
                    countDownLatch.countDown();
                });
                try {
                    countDownLatch.await(30, TimeUnit.SECONDS);
                }catch (Throwable throwable){
                    LOGGER.error("",throwable);
                }
            }

            context.put(MySQLManager.class, mySQLManager);
            mySQLManager = null;
        }

        context.put(DrdsRunner.class, new DrdsRunner(() -> ((MetadataManager) context.get(MetadataManager.class)).getSchemaMap(), PlanCache.INSTANCE));
        MetaClusterCurrent.register(context);
    }
}