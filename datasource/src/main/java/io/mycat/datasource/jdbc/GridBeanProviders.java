package io.mycat.datasource.jdbc;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GridBeanProviders {

  <T extends JdbcDataSource> T createJdbcDataSource(GridRuntime runtime,int index, DatasourceConfig datasourceConfig,
      JdbcReplica mycatReplica);

  <T extends JdbcReplica> T createJdbcReplica(GridRuntime runtime,
      Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex,
      List<DatasourceConfig> datasourceConfigList,
      DatasourceProvider provider);
}