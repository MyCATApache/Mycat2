package io.mycat;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GBeanProviders;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import java.util.List;
import java.util.Set;

public final class DefaultGridBeanProviders implements GBeanProviders {

  @Override
  public <T extends JdbcDataSource> T createJdbcDataSource(GRuntime runtime, int index,
      DatasourceConfig datasourceConfig, JdbcReplica mycatReplica) {
    return (T) new JdbcDataSource(index, datasourceConfig, mycatReplica) {
    };
  }

  @Override
  public <T extends JdbcReplica> T createJdbcReplica(GRuntime runtime, ReplicaConfig replicaConfig,
      Set<Integer> writeIndex,
      List<DatasourceConfig> datasourceConfigList, DatasourceProvider provider) {
    return (T) new JdbcReplica(runtime, replicaConfig, writeIndex,
        datasourceConfigList, provider);
  }
}