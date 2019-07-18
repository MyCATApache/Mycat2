package io.mycat.datasource.jdbc;

import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.ProxyRuntime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcReplica implements MycatReplica {

  private final JdbcDataSourceManager dataSourceManager;
  private final ReplicaDatasourceSelector<JdbcDataSource> selector;
  private ReplicaConfig replicaConfig;

  public JdbcReplica(ProxyRuntime runtime, Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex,DatasourceProvider provider) {
    this.replicaConfig = replicaConfig;
    List<JdbcDataSource> datasourceList = getJdbcDatasourceList(replicaConfig);
    selector = new ReplicaDatasourceSelector<>(runtime, replicaConfig, writeIndex,
        datasourceList);
    this.dataSourceManager = new JdbcDataSourceManager(runtime, provider,jdbcDriverMap,
        datasourceList);
  }

  public static List<JdbcDataSource> getJdbcDatasourceList(ReplicaConfig replicaConfig) {
    List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
    if (mysqls == null) {
      return Collections.emptyList();
    }
    List<JdbcDataSource> datasourceList = new ArrayList<>();
    for (int index = 0; index < mysqls.size(); index++) {
      DatasourceConfig datasourceConfig = mysqls.get(index);
      if (datasourceConfig.getDbType() != null) {
        datasourceList.add(new JdbcDataSource(index, datasourceConfig));
      }
    }
    return datasourceList;
  }

  public JdbcSession getJdbcSessionByBalance(JdbcDataSourceQuery query) {
    JdbcDataSource source = getDataSourceByBalance(query);
    return dataSourceManager.createSession(source);
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
}