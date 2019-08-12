package io.mycat.datasource.jdbc;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface GBeanProviders {

  <T extends JdbcDataSource> T createJdbcDataSource(GRuntime runtime, int index,
      DatasourceConfig datasourceConfig,
      JdbcReplica mycatReplica);

  <T extends JdbcReplica> T createJdbcReplica(GRuntime runtime,
      Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex,
      List<DatasourceConfig> datasourceConfigList,
      DatasourceProvider provider);
}