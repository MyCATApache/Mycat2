package io.mycat;

import io.mycat.config.ClusterConfig;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.MycatRouterConfig;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.metadata.MetadataManager;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.proxy.session.Authenticator;
import io.mycat.proxy.session.AuthenticatorImpl;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigPrepare {
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
    UpdateType updateType = UpdateType.FULL;


    //originalRouterConfig = YamlUtil.load(MycatRouterConfig.class, new FileReader(baseDirectory.resolve("metadata.yml").toFile()));
    public ConfigPrepare(
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


        if (router && !cluster && !users && !sequences && !datasources) {
            updateType = UpdateType.ROUTER;
        } else if (!router && !cluster && users && !sequences && !datasources) {
            updateType = UpdateType.USER;
        } else if (!router && !cluster && !users && sequences && !datasources) {
            updateType = UpdateType.SEQUENCE;
        } else {
            updateType = UpdateType.FULL;
        }

    }

    public void invoke() {

        switch (updateType) {
            case USER: {
                LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
                MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);

                this.authenticator = new AuthenticatorImpl(ops.getUsers().stream().collect(Collectors.toMap(k -> k.getUsername(), v -> v)));
                break;
            }
            case SEQUENCE: {
                LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
                MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);

                this.sequenceGenerator = new SequenceGenerator(ops.getSequences());
                break;
            }
            case ROUTER: {
                LoadBalanceManager loadBalanceManager = MetaClusterCurrent.wrapper(LoadBalanceManager.class);
                MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                this.metadataManager = new MetadataManager(ops.getSchemas(), loadBalanceManager,  MetaClusterCurrent.wrapper(SequenceGenerator.class),  MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class), MetaClusterCurrent.wrapper(JdbcConnectionManager.class), ops.getPrototype());
                break;
            }
            case FULL: {
                MycatRouterConfig mycatRouterConfig = ops.getMycatRouterConfig();

                initBy(mycatRouterConfig);
                break;
            }

        }
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
        this.sequenceGenerator = new SequenceGenerator(mycatRouterConfig.getSequences());
        this.authenticator = new AuthenticatorImpl(mycatRouterConfig.getUsers().stream().collect(Collectors.toMap(k -> k.getUsername(), v -> v)));
        this.metadataManager = new MetadataManager(mycatRouterConfig.getSchemas(), loadBalanceManager, sequenceGenerator, replicaSelector, jdbcConnectionManager, mycatRouterConfig.getPrototype());
    }

    public enum UpdateType {
        USER,
        SEQUENCE,
        ROUTER,
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


        MetaClusterCurrent.register(context);
    }
}