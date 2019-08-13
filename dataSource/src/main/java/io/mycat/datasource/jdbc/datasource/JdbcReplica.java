package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GBeanProviders;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcReplica implements MycatReplica {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcReplica.class);
  protected final ReplicaDataSourceSelector selector;
  private final JdbcConnectionManager dataSourceManager;
  private final GRuntime runtime;
  private final ReplicaConfig replicaConfig;

  public JdbcReplica(GRuntime runtime, Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex, List<DatasourceConfig> datasourceConfigList,
      DatasourceProvider provider) {
    this.runtime = runtime;
    this.replicaConfig = replicaConfig;
    List<JdbcDataSource> dataSources = getJdbcDataSources(datasourceConfigList);
    this.selector = ReplicaSelectorRuntime.INSTCANE.getDataSourceSelector(replicaConfig.getName());
    this.dataSourceManager = new JdbcConnectionManager(runtime, provider, jdbcDriverMap,
        dataSources);
  }

  private List<JdbcDataSource> getJdbcDataSources(
      List<DatasourceConfig> datasourceConfigList) {
    GBeanProviders provider = runtime.getProvider();
    ArrayList<JdbcDataSource> dataSources = new ArrayList<>();
    for (int i = 0; i < datasourceConfigList.size(); i++) {
      DatasourceConfig datasourceConfig = datasourceConfigList.get(i);
      if (datasourceConfig.getDbType() != null && datasourceConfig.getUrl() != null) {
        dataSources.add(provider.createJdbcDataSource(runtime, i, datasourceConfig, this));
      }
    }
    return dataSources;
  }

  public JdbcDataSource getDataSourceByBalance(JdbcDataSourceQuery query) {
    boolean runOnMaster = false;
    LoadBalanceStrategy strategy = null;
    if (query != null) {
      runOnMaster = query.isRunOnMaster();
      strategy = query.getStrategy();
    }
    return selector.getDataSource(runOnMaster, strategy);
  }

  public String getName() {
    return replicaConfig.getName();
  }

  @Override
  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  @Override
  public boolean switchDataSourceIfNeed() {
    return selector.switchDataSourceIfNeed();
  }

  public List<JdbcDataSource> getDatasourceList() {
    return this.dataSourceManager.getDatasourceList();
  }

  public boolean isMaster(JdbcDataSource jdbcDataSource) {
    return selector.isMaster(jdbcDataSource.getIndex());
  }

  private Connection getConnection(JdbcDataSource dataSource) {
    return dataSourceManager.getConnection(dataSource);
  }

  public DsConnection getDefaultConnection(JdbcDataSource dataSource) {
    Connection connection = getConnection(dataSource);
    return new DefaultConnection(connection, dataSource, true,
        Connection.TRANSACTION_REPEATABLE_READ,
        dataSourceManager);
  }

  public DsConnection getConnection(JdbcDataSource dataSource, boolean autocommit,
      int transactionIsolation) {
    Connection connection = getConnection(dataSource);
    return new DefaultConnection(connection, dataSource, autocommit,
        transactionIsolation,
        dataSourceManager);
  }

  public ReplicaSwitchType getSwitchType() {
    return selector.getSwitchType();
  }
}