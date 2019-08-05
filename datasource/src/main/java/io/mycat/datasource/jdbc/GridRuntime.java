package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.beans.mycat.MycatReplica;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLVariables;
import io.mycat.config.ConfigEnum;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.JdbcDriverRootConfig;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.datasource.jdbc.transaction.TransactionProcessUnit;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.MycatSession;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GridRuntime {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(GridRuntime.class);

  final ProxyRuntime proxyRuntime;
  final Map<String, JdbcReplica> jdbcReplicaMap = new HashMap<>();
  final Map<String, JdbcDataNode> jdbcDataNodeMap = new HashMap<>();
  final Map<String, JdbcDataSource> jdbcDataSourceMap = new HashMap<>();
  final GridBeanProviders providers;
  final boolean isJTA;
  private final DatasourceProvider datasourceProvider;

  public GridRuntime(ProxyRuntime proxyRuntime) throws Exception {
    this.proxyRuntime = proxyRuntime;
    String gridBeanProvidersClass = (String) proxyRuntime.getDefContext()
        .getOrDefault("GridBeanProviders", "io.mycat.DefaultGridBeanProviders");
    this.providers = (GridBeanProviders) Class.forName(gridBeanProvidersClass).newInstance();
    ReplicasRootConfig dsConfig = proxyRuntime.getConfig(ConfigEnum.DATASOURCE);
    MasterIndexesRootConfig replicaIndexConfig = proxyRuntime.getConfig(ConfigEnum.REPLICA_INDEX);
    JdbcDriverRootConfig jdbcDriverRootConfig = proxyRuntime.getConfig(ConfigEnum.JDBC_DRIVER);
    String datasourceProviderClass = jdbcDriverRootConfig.getDatasourceProviderClass();
    Objects.requireNonNull(datasourceProviderClass);
    try {
      this.datasourceProvider = (DatasourceProvider) Class.forName(datasourceProviderClass)
          .newInstance();
    } catch (Exception e) {
      throw new MycatException("can not load datasourceProvider:{}", datasourceProviderClass);
    }
    isJTA = datasourceProvider.isJTA();
    initJdbcReplica(dsConfig, replicaIndexConfig, jdbcDriverRootConfig.getJdbcDriver(),
        datasourceProvider);
    DataNodeRootConfig dataNodeRootConfig = proxyRuntime.getConfig(ConfigEnum.DATANODE);
    initJdbcDataNode(dataNodeRootConfig);

    ScheduledExecutorService blockScheduled = Executors.newScheduledThreadPool(1,
        r -> new SessionThread(r));
    HeartbeatRootConfig heartbeatRootConfig = proxyRuntime
        .getConfig(ConfigEnum.HEARTBEAT);
    long period = heartbeatRootConfig.getHeartbeat().getReplicaHeartbeatPeriod();
    TransactionProcessUnit transactionProcessUnit = new TransactionProcessUnit();
    transactionProcessUnit.start();
    blockScheduled.scheduleAtFixedRate(() -> {
      transactionProcessUnit.run(() -> {
        try {
          for (JdbcDataSource value : jdbcDataSourceMap.values()) {
            value.heartBeat();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }, 0, period, TimeUnit.SECONDS);

  }

  public MySQLVariables getVariables() {
    return proxyRuntime.getVariables();
  }

  public Map<String, Object> getDefContext() {
    return proxyRuntime.getDefContext();
  }


  public JdbcReplica getJdbcReplicaByReplicaName(String name) {
    JdbcReplica jdbcReplica = jdbcReplicaMap.get(name);
    Objects.requireNonNull(jdbcReplica);
    return jdbcReplica;
  }

  public JdbcSession getJdbcSessionByDataNodeName(String dataNodeName,
      MySQLIsolation isolation,
      MySQLAutoCommit autoCommit,
      JdbcDataSourceQuery query) {
    Objects.requireNonNull(dataNodeName);
    JdbcDataNode jdbcDataNode = jdbcDataNodeMap.get(dataNodeName);
    JdbcReplica replica = jdbcDataNode.getReplica();
    JdbcSession session = replica
        .getJdbcSessionByBalance(query);
    session.sync(isolation, autoCommit);
    return session;
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
        Set<Integer> replicaIndexes = ProxyRuntime.getReplicaIndexes(masterIndexes, replicaConfig);
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

  public <T> T getConfig(ConfigEnum heartbeat) {
    return (T) proxyRuntime.getConfig(heartbeat);
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return proxyRuntime.getLoadBalanceByBalanceName(name);
  }

  public void updateReplicaMasterIndexesConfig(MycatReplica replica,
      List<MycatDataSource> writeDataSource) {
    proxyRuntime.updateReplicaMasterIndexesConfig(replica, writeDataSource);
  }

  public GridBeanProviders getProvider() {
    return providers;
  }

  public DataNodeSession createDataNodeSession(MycatSession session) {
    if (isJTA) {
      return new JTADataNodeSession(session,this);
    } else {
      return new SimpleDataNodeSession(session,this);
    }
  }

  public DatasourceProvider getDatasourceProvider() {
    return datasourceProvider;
  }

  public List<Map<String, Object>> query(JdbcDataSource dataSource, String sql) {
    JdbcSession session = dataSource.getReplica().createSessionDirectly(dataSource);
    Objects.requireNonNull(session);
    List<Map<String, Object>> resultList;
    try {
      try (JdbcRowBaseIteratorImpl iterator = session.executeQuery(sql)) {
        resultList = iterator.getResultSetMap();
      }
    } finally {
      session.close();
    }
    return resultList;
  }
}