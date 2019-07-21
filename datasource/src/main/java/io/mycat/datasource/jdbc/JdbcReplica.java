package io.mycat.datasource.jdbc;

import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class JdbcReplica implements MycatReplica {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcReplica.class);
  private final JdbcDataSourceManager dataSourceManager;
  private final ReplicaDatasourceSelector<JdbcDataSource> selector;
  private final GridRuntime runtime;
  private final ReplicaConfig replicaConfig;

  public JdbcReplica(GridRuntime runtime, Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex, List<DatasourceConfig> datasourceConfigList,
      DatasourceProvider provider) {
    this.runtime = runtime;
    this.replicaConfig = replicaConfig;
    List<JdbcDataSource> dataSources = getJdbcDataSources(datasourceConfigList);
    this.selector = new ReplicaDatasourceSelector<>(runtime, replicaConfig, writeIndex, dataSources,
        runtime.getLoadBalanceByBalanceName(null));
    this.dataSourceManager = new JdbcDataSourceManager(runtime, provider, jdbcDriverMap,
        dataSources);
  }

  private List<JdbcDataSource> getJdbcDataSources(
      List<DatasourceConfig> datasourceConfigList) {
    ArrayList<JdbcDataSource> dataSources = new ArrayList<>();
    for (int i = 0; i < datasourceConfigList.size(); i++) {
      DatasourceConfig datasourceConfig = datasourceConfigList.get(i);
      dataSources.add(new JdbcDataSource(i, datasourceConfig, this));
    }
    return dataSources;
  }

  public JdbcSession createSession(JdbcDataSource dataSource) {
    return dataSourceManager.createSession(dataSource);
  }


  public JdbcSession getJdbcSessionByBalance(JdbcDataSourceQuery query) {
    JdbcDataSource source = getDataSourceByBalance(query);
    return createSession(source);
  }

  public JdbcDataSource getDataSourceByBalance(JdbcDataSourceQuery query) {
    boolean runOnMaster = false;
    LoadBalanceStrategy strategy = null;

    if (query != null) {
      runOnMaster = query.isRunOnMaster();
      strategy = query.getStrategy();
    }

    if (strategy == null) {
      strategy = selector.defaultLoadBalanceStrategy;
    }

    if (runOnMaster) {
      return selector.getWriteDatasource(strategy);
    }
    JdbcDataSource datasource;
    List activeDataSource = selector.getDataSourceByLoadBalacneType();
    datasource = (JdbcDataSource) strategy.select(selector, activeDataSource);
    if (datasource == null) {
      datasource = selector.getWriteDatasource(strategy);
      return datasource;
    }
    return datasource;
  }

  public String getName() {
    return replicaConfig.getName();
  }

  public ReplicaConfig getConfig() {
    return replicaConfig;
  }


  @Override
  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  @Override
  public boolean switchDataSourceIfNeed() {
    CopyOnWriteArrayList<JdbcDataSource> writeDataSource = this.selector.writeDataSource;
    switch (this.getConfig().getRepType()) {
      case SINGLE_NODE:
      case MASTER_SLAVE: {
        for (int i = 0; i < writeDataSource.size(); i++) {
          if (writeDataSource.get(i).isAlive()) {
            LOGGER.info("{} switch master to {}", this, i);
            ///////////////////////////////
            runtime.updateReplicaMasterIndexesConfig(this,(List) writeDataSource);
            //////////////////////////////
            return true;
          }
        }
      }
      case GARELA_CLUSTER:
        List<JdbcDataSource> remove = new ArrayList<>();
        for (int i = 0; i < writeDataSource.size(); i++) {
          JdbcDataSource datasource = writeDataSource.get(i);
          if (!datasource.isAlive()) {
            remove.add(datasource);
          }
        }
        writeDataSource.removeAll(remove);
        break;
    }
    return false;
  }
}