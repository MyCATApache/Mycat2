package io.mycat.replica;

import io.mycat.GlobalConst;

public enum ReplicaType {
  // 单一节点
  SINGLE_NODE(GlobalConst.SINGLE_NODE_HEARTBEAT_SQL, new String[]{}),
  // 普通主从
  MASTER_SLAVE(GlobalConst.MASTER_SLAVE_HEARTBEAT_SQL, GlobalConst.MYSQL_SLAVE_STAUTS_COLMS),
  // 普通基于garela cluster集群
  GARELA_CLUSTER(GlobalConst.GARELA_CLUSTER_HEARTBEAT_SQL,
      GlobalConst.MYSQL_CLUSTER_STAUTS_COLMS),
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
  