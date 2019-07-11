package io.mycat.datasource.jdbc;

import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.proxy.ProxyRuntime;
import java.util.Set;

public class JdbcReplica implements MycatReplica {

  private final JdbcDataSourceManager dataSourceManager;
  private JdbcReplicaDatasourceSelector selector;

  public JdbcReplica(ProxyRuntime runtime,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex) {
    selector = new JdbcReplicaDatasourceSelector(runtime, replicaConfig, writeIndex);
    this.dataSourceManager = new JdbcDataSourceManager(runtime, DatasourceProviderImpl.INSTANCE,
        selector.datasourceList);
  }

  public JdbcSession getJdbcSessionByBalance(JdbcDataSourceQuery query) {
    JdbcDataSource source = selector.getDataSourceByBalance(query);
    return dataSourceManager.createSession(source);
  }

}