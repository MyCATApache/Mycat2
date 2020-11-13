package io.mycat;

import io.mycat.config.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt4.PlanCache;
import io.mycat.metadata.MetadataManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.replica.ReplicaSelectorRuntime;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigPrepareExecuter {
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

                this.authenticator = new AuthenticatorImpl(ops.getUsers().stream().collect(Collectors.toMap(k -> k.getUsername(), v -> v)));
                break;
            }
            case SEQUENCE: {
                LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
                MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                ServerConfig serverConfig = MetaClusterCurrent.wrapper(MycatServerConfig.class).getServer();
                this.sequenceGenerator = new SequenceGenerator(serverConfig.getWorkId(),ops.getSequences());
                break;
            }
            case ROUTER: {
                this.metadataManager = createMetaData();
                break;
            }
            case CREATE_TABLE: {
                String schemaName = ops.getSchemaName();
                String tableName = ops.getTableName();
                this.metadataManager = createMetaData();
                TableHandler table = this.metadataManager.getTable(schemaName, tableName);
                table.createPhysicalTables();
                break;
            }
            case DROP_TABLE: {
                MetadataManager oldMetadataManager= MetaClusterCurrent.wrapper(MetadataManager.class);

                String schemaName = ops.getSchemaName();
                String tableName = ops.getTableName();
                this.metadataManager = createMetaData();

                TableHandler table = oldMetadataManager.getTable(schemaName, tableName);
                if (table != null) {
                    table.dropPhysicalTables();
                }
                break;
            }
            case FULL: {
                MycatRouterConfig mycatRouterConfig = ops.getMycatRouterConfig();
                initBy(mycatRouterConfig);
                break;
            }

        }
    }

    @NotNull
    private MetadataManager createMetaData() {
        return new MetadataManager(ops.getSchemas(),
                MetaClusterCurrent.wrapper(LoadBalanceManager.class),
                MetaClusterCurrent.wrapper(SequenceGenerator.class),
                MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class),
                MetaClusterCurrent.wrapper(JdbcConnectionManager.class),
                "prototype");
    }

    public void initBy(MycatRouterConfig mycatRouterConfig) {
        if (MetaClusterCurrent.exist(ReplicaSelectorRuntime.class)) {
            ReplicaSelectorRuntime replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
            replicaSelectorRuntime.close();
        }
        LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        Map<String, DatasourceConfig> datasourceConfigMap = mycatRouterConfig.getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        Map<String, ClusterConfig> clusters = mycatRouterConfig.getClusters().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        replicaSelector = new ReplicaSelectorRuntime(mycatRouterConfig.getClusters(), datasourceConfigMap, loadBalanceManager, metadataStorageManager);
        jdbcConnectionManager = new JdbcConnectionManager(datasourceProvider, datasourceConfigMap, clusters, mycatWorkerProcessor, replicaSelector);
        datasourceConfigProvider = new DatasourceConfigProvider() {
            @Override
            public Map<String, DatasourceConfig> get() {
                return datasourceConfigMap;
            }
        };
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(MycatServerConfig.class).getServer();
        this.sequenceGenerator = new SequenceGenerator(serverConfig.getWorkId(), mycatRouterConfig.getSequences());
        this.authenticator = new AuthenticatorImpl(mycatRouterConfig.getUsers().stream().collect(Collectors.toMap(k -> k.getUsername(), v -> v)));
        this.metadataManager = new MetadataManager(mycatRouterConfig.getSchemas(), loadBalanceManager, sequenceGenerator, replicaSelector, jdbcConnectionManager, mycatRouterConfig.getPrototype());
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

        HashMap<Class, Object> context = new HashMap<>();

        Map<Class, Object> oldContext = MetaClusterCurrent.context.get();

        if (oldContext != null) {
            context.putAll(oldContext);
        }

        if (replicaSelector != null) {
            context.put(replicaSelector.getClass(), replicaSelector);
        }
        if (jdbcConnectionManager != null) {
            context.put(jdbcConnectionManager.getClass(), jdbcConnectionManager);
        }
        if (metadataManager != null) {
            context.put(metadataManager.getClass(), metadataManager);
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
        }
        if (sequenceGenerator != null) {
            context.put(sequenceGenerator.getClass(), sequenceGenerator);
        }
        context.put(MetadataStorageManager.class, this.metadataStorageManager);
        context.put(MysqlVariableService.class, this.metadataManager);
        MycatRouterConfig mycatRouterConfig = ops.getMycatRouterConfig();
        context.put(MycatRouterConfig.class, mycatRouterConfig);
        context.put(DrdsRunner.class,new DrdsRunner(() -> this.metadataManager.getSchemaMap(), PlanCache.INSTANCE));
        MetaClusterCurrent.register(context);
    }
}