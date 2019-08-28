package io.mycat.replica;

import io.mycat.config.GlobalConfig;

public enum ReplicaType {
  // 单一节点
  SINGLE_NODE(GlobalConfig.SINGLE_NODE_HEARTBEAT_SQL, new String[]{}),
  // 普通主从
  MASTER_SLAVE(GlobalConfig.MASTER_SLAVE_HEARTBEAT_SQL, GlobalConfig.MYSQL_SLAVE_STAUTS_COLMS),
  // 普通基于garela cluster集群
  GARELA_CLUSTER(GlobalConfig.GARELA_CLUSTER_HEARTBEAT_SQL,
      GlobalConfig.MYSQL_CLUSTER_STAUTS_COLMS),
  NONE("SELECT 1;", null),
  ;

  private byte[] hearbeatSQL;
  private String[] fetchColms;

  ReplicaType(String hearbeatSQL, String[] fetchColms) {
    this.hearbeatSQL = hearbeatSQL.getBytes();
    this.fetchColms = fetchColms;
  }

  public byte[] getHearbeatSQL() {
    return hearbeatSQL;
  }

  public String[] getFetchColms() {
    return fetchColms;
  }
}
  