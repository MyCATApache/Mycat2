package io.mycat.replica;

import io.mycat.config.datasource.ReplicaConfig;

/**
 * @author jamie12221
 * @date 2019-05-14 19:19
 **/
public interface MySQLReplicaFactory {

  MySQLReplica get(ReplicaConfig replicaConfig, int writeIndex);
}
