package io.mycat.datasource.jdbc;

import io.mycat.ModuleUtil;
import io.mycat.MycatException;
import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiver;
import io.mycat.config.ConfigurableRoot;
import io.mycat.config.GlobalConfig;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.JdbcDriverRootConfig;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.plug.PlugRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataNode;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcDataSourceQuery;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import io.mycat.datasource.jdbc.manager.TransactionProcessJob;
import io.mycat.datasource.jdbc.manager.TransactionProcessKey;
import io.mycat.datasource.jdbc.manager.TransactionProcessUnit;
import io.mycat.datasource.jdbc.manager.TransactionProcessUnitManager;
import io.mycat.datasource.jdbc.session.JTATransactionSessionImpl;
import io.mycat.datasource.jdbc.session.LocalTransactionSessionImpl;
import io.mycat.datasource.jdbc.session.TransactionSession;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public enum GRuntime {
  INSTACNE;
  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(GRuntime.class);
  private final Map<String, JdbcReplica> jdbcReplicaMap = new HashMap<>();
  private final Map<String, JdbcDataNode> jdbcDataNodeMap = new HashMap<>();
  private final Map<String, JdbcDataSource> jdbcDataSourceMap = new HashMap<>();
  private final GBeanProviders providers;
  private final boolean isJTA;
  private final DatasourceProvider datasourceProvider;
  private final LoadBalanceManager loadBalanceManager = new LoadBalanceManager();
  private final ConfigReceiver config;
  private final TransactionProcessUnitManager manager;

  GRuntime() {
    try {
      this.config = ConfigLoader
          .load("D:\\newgit2\\mycat2\\mycat2\\src\\main\\resources", GlobalConfig.genVersion());
      String gridBeanProvidersClass = "io.mycat.DefaultGridBeanProviders";
      this.providers = (GBeanProviders) Class.forName(gridBeanProvidersClass).newInstance();
      ReplicasRootConfig dsConfig = config.getConfig(ConfigEnum.DATASOURCE);
      MasterIndexesRootConfig replicaIndexConfig = config.getConfig(ConfigEnum.REPLICA_INDEX);
      JdbcDriverRootConfig jdbcDriverRootConfig = config.getConfig(ConfigEnum.JDBC_DRIVER);
      String datasourceProviderClass = jdbcDriverRootConfig.getDatasourceProviderClass();
      Objects.requireNonNull(datasourceProviderClass);

      PlugRootConfig plugRootConfig = config.getConfig(ConfigEnum.PLUG);
      Objects.requireNonNull(plugRootConfig, "plug config can not found");
      loadBalanceManager.load(plugRootConfig);

      try {
        this.datasourceProvider = (DatasourceProvider) Class.forName(datasourceProviderClass)
            .newInstance();
      } catch (Exception e) {
        throw new MycatException("can not load datasourceProvider:{}", datasourceProviderClass);
      }
      isJTA = datasourceProvider.isJTA();
      initJdbcReplica(dsConfig, replicaIndexConfig, jdbcDriverRootConfig.getJdbcDriver(),
          datasourceProvider);
      DataNodeRootConfig dataNodeRootConfig = config.getConfig(ConfigEnum.DATANODE);
      initJdbcDataNode(dataNodeRootConfig);
      manager = new TransactionProcessUnitManager(this);
    } catch (Exception e) {
      throw new MycatException(e);
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
      DatasourceProvider datasourceProvider)
      throws Exception {
    Map<String, String> masterIndexes = replicaIndexConfig.getMasterIndexes();
    Objects.requireNonNull(jdbcDriverMap, "jdbcDriver.yml is not existed.");
    for (String valve : jdbcDriverMap.values()) {
      Class.forName(valve);
    }
    if (replicasRootConfig != null && replicasRootConfig.getReplicas() != null
        && !replicasRootConfig.getReplicas().isEmpty()) {
      for (ReplicaConfig replicaConfig : replicasRootConfig.getReplicas()) {
        Set<Integer> replicaIndexes = ModuleUtil.getReplicaIndexes(masterIndexes, replicaConfig);
        List<DatasourceConfig> jdbcDatasourceConfigList = getJdbcDatasourceList(replicaConfig);
        if (jdbcDatasourceConfigList.isEmpty()) {
          continue;
        }
        JdbcReplica jdbcReplica = providers.createJdbcReplica(this, jdbcDriverMap, replicaConfig,
            replicaIndexes, jdbcDatasourceConfigList, datasourceProvider);
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
        try {
          if (jdbcReplica == null) {
            continue;
          }
          jdbcDataNodeMap.put(dataNode.getName(),
              new JdbcDataNode(jdbcReplica, dataNode));
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
    }
  }

  private static List<DatasourceConfig> getJdbcDatasourceList(ReplicaConfig replicaConfig) {
    List<DatasourceConfig> mysqls = replicaConfig.getDatasources();
    if (mysqls == null) {
      return Collections.emptyList();
    }
    List<DatasourceConfig> datasourceList = new ArrayList<>();
    for (int index = 0; index < mysqls.size(); index++) {
      DatasourceConfig datasourceConfig = mysqls.get(index);
      if (datasourceConfig.getUrl() != null) {
        datasourceList.add(datasourceConfig);
      }
    }
    return datasourceList;
  }

  public GBeanProviders getProvider() {
    return providers;
  }

  public TransactionSession createTransactionSession(TransactionProcessUnit processUnit) {
    if (isJTA) {
      return new LocalTransactionSessionImpl(processUnit);
    } else {
      return new JTATransactionSessionImpl(datasourceProvider.createUserTransaction(), processUnit);
    }
  }

  public DatasourceProvider getDatasourceProvider() {
    return datasourceProvider;
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return loadBalanceManager.getLoadBalanceByBalanceName(name);
  }

  public void updateReplicaMasterIndexesConfig(JdbcReplica replica, List writeDataSource) {
  }

  public <T extends ConfigurableRoot> T getConfig(ConfigEnum configEnum) {
    ConfigurableRoot config = this.config.getConfig(configEnum);
    return (T) config;
  }

  public void run(TransactionProcessKey key, TransactionProcessJob processTask) {
    manager.run(key, processTask);
  }
}