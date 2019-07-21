package io.mycat;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GridBeanProviders;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.datasource.jdbc.JdbcReplica;
import io.mycat.replica.JdbcDataSourceEx;
import io.mycat.replica.JdbcReplicaEx;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class DefaultGridBeanProviders implements GridBeanProviders {

  @Override
  public <T extends JdbcDataSource> T createJdbcDataSource(GridRuntime runtime, int index,
      DatasourceConfig datasourceConfig, JdbcReplica mycatReplica) {
    return (T)new JdbcDataSourceEx(runtime, index, datasourceConfig, mycatReplica);
  }

  @Override
  public <T extends JdbcReplica> T createJdbcReplica(GridRuntime runtime,
      Map<String, String> jdbcDriverMap, ReplicaConfig replicaConfig, Set<Integer> writeIndex,
      List<DatasourceConfig> datasourceConfigList, DatasourceProvider provider) {
    return (T)new JdbcReplicaEx(runtime,jdbcDriverMap,replicaConfig,writeIndex,datasourceConfigList,provider);
  }
}