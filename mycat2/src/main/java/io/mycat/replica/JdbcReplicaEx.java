package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcReplicaEx extends JdbcReplica {

  public JdbcReplicaEx(GRuntime runtime,
      Map<String, String> jdbcDriverMap,
      ReplicaConfig replicaConfig,
      Set<Integer> writeIndex,
      List<DatasourceConfig> datasourceConfigList, DatasourceProvider datasourceProvider) {
    super(runtime, jdbcDriverMap, replicaConfig, writeIndex, datasourceConfigList, datasourceProvider);
  }
}