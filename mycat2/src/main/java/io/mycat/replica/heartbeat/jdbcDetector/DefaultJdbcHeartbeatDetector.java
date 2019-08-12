package io.mycat.replica.heartbeat.jdbcDetector;

import io.mycat.api.collector.CommonSQLCallback;
import io.mycat.config.ConfigFile;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.DsConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import io.mycat.replica.heartbeat.HeartbeatManager;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DefaultJdbcHeartbeatDetector implements
    HeartbeatDetector<JdbcDataSource, CommonSQLCallback> {

  private final JdbcReplica replica;
  private final JdbcDataSource jdbcDataSource;
  private final HeartbeatManager manager;
  private final CommonSQLCallback callback;
  protected volatile long lastSendQryTime;
  protected volatile long lastReceivedQryTime;//    private isCheck
  protected final long heartbeatTimeout;

  public DefaultJdbcHeartbeatDetector(GRuntime runtime, JdbcReplica replica,
      JdbcDataSource jdbcDataSource, HeartbeatManager manager,
      Function<HeartbeatDetector, CommonSQLCallback> commonSQLCallbacbProvider) {
    this.replica = replica;
    this.jdbcDataSource = jdbcDataSource;
    this.manager = manager;
    this.callback = commonSQLCallbacbProvider.apply(this);

    HeartbeatRootConfig heartbeatRootConfig = runtime.getConfig(
        ConfigFile.HEARTBEAT);
    HeartbeatConfig heartbeatConfig = heartbeatRootConfig
        .getHeartbeat();
    this.heartbeatTimeout = heartbeatConfig.getMinHeartbeatChecktime();
  }

  @Override
  public ReplicaConfig getReplicaConfig() {
    return replica.getReplicaConfig();
  }

  @Override
  public JdbcDataSource getDataSource() {
    return jdbcDataSource;
  }

  @Override
  public HeartbeatManager getHeartbeatManager() {
    return manager;
  }

  @Override
  public void heartBeat() {
    DsConnection connection = null;
    try {
      connection = replica.getDefaultConnection(jdbcDataSource);
      List<Map<String, Object>> resultList;
      try (JdbcRowBaseIteratorImpl iterator = connection.executeQuery(callback.getSql())) {
        resultList = iterator.getResultSetMap();
      }
      callback.process(resultList);
    } catch (Exception e) {
      callback.onException(e);
      throw e;
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }


  @Override
  public CommonSQLCallback getCallback() {
    return callback;
  }

  @Override
  public boolean isHeartbeatTimeout() {
    return System.currentTimeMillis() > Math.max(lastSendQryTime,
        lastReceivedQryTime) + heartbeatTimeout;
  }

  @Override
  public void updateLastReceivedQryTime() {
    this.lastReceivedQryTime = System.currentTimeMillis();
  }

  @Override
  public void updateLastSendQryTime() {
    this.lastSendQryTime = System.currentTimeMillis();
  }

  @Override
  public boolean quitDetector() {
    return false;
  }
}