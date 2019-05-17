package io.mycat.replica;

import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.replica.heart.MySQLReplicaEx;

/**
 * @author jamie12221
 * @date 2019-05-14 19:26
 **/
public class DefaultMySQLReplicaFactory implements MySQLReplicaFactory {

  @Override
  public MySQLReplica get(ReplicaConfig replicaConfig, int writeIndex) {
    return new MySQLReplicaEx(replicaConfig, writeIndex, new DefaultDataSourceFactory()) {
    };
  }
}
