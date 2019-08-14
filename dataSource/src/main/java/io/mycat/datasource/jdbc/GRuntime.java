package io.mycat.datasource.jdbc;


import io.mycat.ConfigRuntime;
import io.mycat.MycatException;
import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadCallback;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.config.ConfigFile;
import io.mycat.config.ConfigReceiver;
import io.mycat.config.ConfigurableRoot;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.JdbcDriverRootConfig;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.config.plug.PlugRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.datasource.jdbc.datasource.DsConnection;
import io.mycat.datasource.jdbc.datasource.JTATransactionSessionImpl;
import io.mycat.datasource.jdbc.datasource.JdbcDataNode;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcDataSourceQuery;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import io.mycat.datasource.jdbc.datasource.LocalTransactionSessionImpl;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.datasource.jdbc.thread.GThreadPool;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.replica.ReplicaHeartbeatRuntime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public enum GRuntime {
  INSTACNE;
  private final MycatLogger LOGGER = MycatLoggerFactory.getLogger(GRuntime.class);
  private final Map<String, JdbcReplica> jdbcReplicaMap = new HashMap<>();
  private final Map<String, JdbcDataNode> jdbcDataNodeMap = new HashMap<>();
  private final Map<String, JdbcDataSource> jdbcDataSourceMap = new HashMap<>();
  private final Map<String, Object> defContext = new HashMap<>();
  private GBeanProviders providers;
  private boolean isJTA;
  private DatasourceProvider datasourceProvider;
  private ConfigReceiver config;
  private GThreadPool gThreadPool;
  private ScheduledExecutorService schedule;

  GRuntime() {

  }


  public void load(ConfigReceiver config) {
    this.config = config;
    String gridBeanProvidersClass = "io.mycat.DefaultGridBeanProviders";
    try {
      this.providers = (GBeanProviders) Class.forName(gridBeanProvidersClass).newInstance();
    } catch (Exception e) {
      LOGGER.error("", e);
    }
    ReplicasRootConfig dsConfig = config.getConfig(ConfigFile.DATASOURCE);
    MasterIndexesRootConfig replicaIndexConfig = config.getConfig(ConfigFile.REPLICA_INDEX);
    JdbcDriverRootConfig jdbcDriverRootConfig = config.getConfig(ConfigFile.JDBC_DRIVER);
    String datasourceProviderClass = jdbcDriverRootConfig.getDatasourceProviderClass();
    Objects.requireNonNull(datasourceProviderClass);
    PlugRootConfig plugRootConfig = config.getConfig(ConfigFile.PLUG);
    Objects.requireNonNull(plugRootConfig, "plug config can not found");
    try {
      this.datasourceProvider = (DatasourceProvider) Class.forName(datasourceProviderClass)
          .newInstance();
    } catch (Exception e) {
      throw new MycatException("can not load datasourceProvider:{}", datasourceProviderClass);
    }
    isJTA = datasourceProvider.isJTA();
    initJdbcReplica(dsConfig, replicaIndexConfig, jdbcDriverRootConfig.getJdbcDriver(),
        datasourceProvider);
    DataNodeRootConfig dataNodeRootConfig = config.getConfig(ConfigFile.DATANODE);
    initJdbcDataNode(dataNodeRootConfig);
    gThreadPool = new GThreadPool(this);

    initHeartbeat(dsConfig);
  }

  private void initHeartbeat(ReplicasRootConfig dsConfig) {
    HeartbeatRootConfig heartbeatRootConfig = getConfig(ConfigFile.HEARTBEAT);
    HeartbeatConfig heartbeatConfig = heartbeatRootConfig.getHeartbeat();
    boolean existUpdate = false;
    for (ReplicaConfig replica : dsConfig.getReplicas()) {
      List<DatasourceConfig> datasources = replica.getDatasources();
      if (datasources != null) {
        datasources = Collections.emptyList();
      }
      final BindThreadKey key = new BindThreadKey() {
      };
      for (DatasourceConfig datasource : Objects.requireNonNull(datasources)) {
        existUpdate = existUpdate || ReplicaHeartbeatRuntime.INSTANCE
            .register(replica, datasource, heartbeatConfig,
                heartBeatStrategy -> {
                  run(key, new BindThreadCallback() {
                    @Override
                    public void accept(BindThreadKey key, BindThread context) {
                      JdbcDataSource jdbcDataSource = jdbcDataSourceMap.get(datasource.getName());
                      if (jdbcDataSource != null) {
                        DsConnection connection = null;
                        try {
                          connection = jdbcReplicaMap.get(replica.getName())
                              .getDefaultConnection(jdbcDataSource);
                          List<Map<String, Object>> resultList;
                          try (JdbcRowBaseIteratorImpl iterator = connection
                              .executeQuery(heartBeatStrategy.getSql())) {
                            resultList = iterator.getResultSetMap();
                          }
                          heartBeatStrategy.process(resultList);
                        } catch (Exception e) {
                          heartBeatStrategy.onException(e);
                          throw e;
                        } finally {
                          if (connection != null) {
                            connection.close();
                          }
                        }
                      }
                    }

                    @Override
                    public void onException(BindThreadKey key, Exception e) {
                      heartBeatStrategy.onException(e);
                    }
                  });

                });
      }
    }
    if (existUpdate) {
      long period = heartbeatConfig.getReplicaHeartbeatPeriod();
      if (schedule == null) {
        schedule = Executors.newScheduledThreadPool(1);
      }
      schedule.scheduleAtFixedRate(() -> {
            try {
              ReplicaHeartbeatRuntime.INSTANCE.heartbeat();
            } catch (Exception e) {
              LOGGER.error("", e);
            }
          }, 0, period,
          TimeUnit.MILLISECONDS);
    }
  }

  public JdbcReplica getJdbcReplicaByReplicaName(String name) {
    JdbcReplica jdbcReplica = jdbcReplicaMap.get(name);
    Objects.requireNonNull(jdbcReplica);
    return jdbcReplica;
  }

  public JdbcDataSource getJdbcDatasourceByDataNodeName(String dataNodeName,
      JdbcDataSourceQuery query) {
    Objects.requireNonNull(dataNodeName);
    JdbcDataNode jdbcDataNode = jdbcDataNodeMap.get(dataNodeName);
    JdbcReplica replica = jdbcDataNode.getReplica();
    return replica.getDataSourceByBalance(query);
  }


  private void initJdbcReplica(ReplicasRootConfig replicasRootConfig,
      MasterIndexesRootConfig replicaIndexConfig, Map<String, String> jdbcDriverMap,
      DatasourceProvider datasourceProvider) {
    Map<String, String> masterIndexes = replicaIndexConfig.getMasterIndexes();
    Objects.requireNonNull(jdbcDriverMap, "jdbcDriver.yml is not existed.");
    for (String valve : jdbcDriverMap.values()) {
      try {
        Class.forName(valve);
      } catch (ClassNotFoundException e) {
        LOGGER.error("", e);
      }
    }
    if (replicasRootConfig != null && replicasRootConfig.getReplicas() != null
        && !replicasRootConfig.getReplicas().isEmpty()) {
      for (ReplicaConfig replicaConfig : replicasRootConfig.getReplicas()) {
        Set<Integer> replicaIndexes = ConfigRuntime.INSTCANE
            .getReplicaIndexes(replicaConfig.getName());
        JdbcReplica jdbcReplica = providers.createJdbcReplica(this, jdbcDriverMap, replicaConfig,
            replicaIndexes, replicaConfig.getDatasources(), datasourceProvider);
        jdbcReplicaMap.put(jdbcReplica.getName(), jdbcReplica);
        List<JdbcDataSource> datasourceList = jdbcReplica.getDatasourceList();
        for (JdbcDataSource jdbcDataSource : datasourceList) {
          jdbcDataSourceMap.compute(jdbcDataSource.getName(),
              (s, dataSource) -> {
                if (dataSource != null) {
                  throw new MycatException("duplicate name of jdbc datasource");
                }
                return jdbcDataSource;
              });
        }
      }
    }
  }

  private void initJdbcDataNode(DataNodeRootConfig config) {
    if (config != null && config.getDataNodes() != null) {
      List<DataNodeConfig> dataNodes = config.getDataNodes();
      for (DataNodeConfig dataNode : dataNodes) {
        JdbcReplica jdbcReplica = jdbcReplicaMap.get(dataNode.getReplica());
        if (jdbcReplica == null) {
          continue;
        }
        JdbcDataNode jdbcDataNode = new JdbcDataNode(jdbcReplica, dataNode);
        jdbcDataNodeMap.put(dataNode.getName(), jdbcDataNode);
      }
    }
  }


  public GBeanProviders getProvider() {
    return providers;
  }

  public TransactionSession createTransactionSession(GThread gThread) {
    if (isJTA) {
      return new JTATransactionSessionImpl(datasourceProvider.createUserTransaction(), gThread);
    } else {
      return new LocalTransactionSessionImpl(gThread);
    }
  }

  public DatasourceProvider getDatasourceProvider() {
    return datasourceProvider;
  }

  public <T extends ConfigurableRoot> T getConfig(ConfigFile configEnum) {
    ConfigurableRoot config = this.config.getConfig(configEnum);
    return (T) config;
  }

  public <K extends BindThreadKey, T extends BindThreadCallback> void run(K key, T processTask) {
    gThreadPool.run(key, processTask);
  }

  public Map<String, Object> getDefContext() {
    return defContext;
  }
}