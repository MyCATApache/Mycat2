package io.mycat.replica;

import io.mycat.MycatProxyBeanProviders;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.MySQLReplica;
import java.util.List;
import java.util.Set;

/**
 * @author jamie12221
 *  date 2019-05-17 13:11
 **/
public class MySQLReplicaEx extends MySQLReplica {


  /**
   * 初始化mycat集群管理
   * @param replicaConfig
   * @param writeIndex
   * @param dataSourceFactory
   */
  public MySQLReplicaEx(ProxyRuntime runtime,ReplicaConfig replicaConfig, Set<Integer> writeIndex,
      MycatProxyBeanProviders dataSourceFactory) {
    super(runtime,replicaConfig, writeIndex, dataSourceFactory);
  }

  @Override
  public ReplicaConfig getReplicaConfig() {
    return config;
  }

  public boolean switchDataSourceIfNeed() {
      return super.switchDataSourceIfNeed();
  }
}
