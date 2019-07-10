package io.mycat.datasource.jdbc;

import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class JdbcReplica implements MycatReplica {

  static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcReplica.class);
  private final ReplicaConfig config;
  private final List<JdbcDataSource> datasourceList = new ArrayList<>();
  private final CopyOnWriteArrayList<JdbcDataSource> writeDataSource = new CopyOnWriteArrayList<>(); //主节点默认为0
  private LoadBalanceStrategy defaultLoadBalanceStrategy;

  public JdbcReplica(ReplicaConfig config) {
    this.config = config;
  }

  public JdbcSession getJdbcSessionByBalance() {

    return null;
  }
}