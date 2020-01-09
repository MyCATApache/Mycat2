/**
 * Copyright (C) <2020>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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
  