package io.mycat.datasource.jdbc;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.config.ConfigEnum;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GridRuntime {

  final ProxyRuntime proxyRuntime;
  final Map<String, JdbcReplica> jdbcReplicaMap = new HashMap<>();
  final Map<String, JdbcDataNode> jdbcDataNodeMap = new HashMap<>();


  public GridRuntime(ProxyRuntime proxyRuntime) {
    this.proxyRuntime = proxyRuntime;
    ReplicasRootConfig dsConfig = proxyRuntime.getConfig(ConfigEnum.DATASOURCE);
    MasterIndexesRootConfig replicaIndexConfig = proxyRuntime.getConfig(ConfigEnum.REPLICA_INDEX);
    initJdbcReplica(dsConfig, replicaIndexConfig);

    DataNodeRootConfig dataNodeRootConfig = proxyRuntime.getConfig(ConfigEnum.DATANODE);
    initJdbcDataNode(dataNodeRootConfig);
  }

  public JdbcSession getJdbcSessionByDataNodeName(String dataNodeName,
      String schema, MySQLIsolation isolation,
      MySQLAutoCommit autoCommit, String charset,
      JdbcDataSourceQuery query) {
    JdbcSession session = jdbcDataNodeMap.get(dataNodeName).getReplica()
        .getJdbcSessionByBalance(query);
    session.sync(schema, isolation, autoCommit, charset);
    return session;
  }

  public JdbcSession getJdbcSessionByDataNodeName(String dataNodeName,
      JdbcSyncContext context,
      JdbcDataSourceQuery query) {
    JdbcDataNode jdbcDataNode = jdbcDataNodeMap.get(dataNodeName);
    JdbcReplica replica = jdbcDataNode.getReplica();
    JdbcSession session = replica
        .getJdbcSessionByBalance(query);
    session.sync(jdbcDataNode.getDatabase(), context.getIsolation(), context.getAutoCommit(),
        context.getCharset());
    return session;
  }

  public JdbcSession getJdbcSessionByDataNodeName(MycatSession mycat, String dataNodeName,
      JdbcDataSourceQuery query) {
    JdbcDataNode jdbcDataNode = jdbcDataNodeMap.get(dataNodeName);
    JdbcReplica replica = jdbcDataNode.getReplica();
    JdbcSession session = replica
        .getJdbcSessionByBalance(query);
    session.sync(jdbcDataNode.getDatabase(), mycat.getIsolation(), mycat.getAutoCommit(),
        mycat.getCharsetName());
    return session;
  }

  private void initJdbcReplica(ReplicasRootConfig replicasRootConfig,
      MasterIndexesRootConfig replicaIndexConfig) {
    Map<String, String> masterIndexes = replicaIndexConfig.getMasterIndexes();
    if (replicasRootConfig != null && replicasRootConfig.getReplicas() != null
        && !replicasRootConfig.getReplicas().isEmpty()) {
      for (ReplicaConfig replicaConfig : replicasRootConfig.getReplicas()) {
        Set<Integer> replicaIndexes = ProxyRuntime.getReplicaIndexes(masterIndexes, replicaConfig);
        JdbcReplica jdbcReplica = new JdbcReplica(proxyRuntime, replicaConfig, replicaIndexes);
        jdbcReplicaMap.put(jdbcReplica.getName(), jdbcReplica);
      }
    }
  }

  private void initJdbcDataNode(DataNodeRootConfig config) {
    if (config != null && config.getDataNodes() != null) {
      List<DataNodeConfig> dataNodes = config.getDataNodes();
      for (DataNodeConfig dataNode : dataNodes) {
        jdbcDataNodeMap.put(dataNode.getName(),
            new JdbcDataNode(jdbcReplicaMap.get(dataNode.getReplica()), dataNode));
      }
    }

  }
}