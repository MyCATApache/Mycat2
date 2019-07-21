package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcReplica;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcReplicaEx extends JdbcReplica {

  public JdbcReplicaEx(GridRuntime runtime,
      Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex,
      List<DatasourceConfig> datasourceConfigList, DatasourceProvider datasourceProvider) {
    super(runtime, jdbcDriverMap, replicaConfig, writeIndex, datasourceConfigList, datasourceProvider);
  }
}