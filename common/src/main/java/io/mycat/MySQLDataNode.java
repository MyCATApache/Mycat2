/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat;

import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.schema.DataNodeConfig;
import java.util.Objects;

/**
 * @author jamie12221
 * @date 2019-05-04 15:13
 **/
public class MySQLDataNode extends MycatDataNode {
  public MycatReplica replica;
  private DataNodeConfig dataNodeConfig;

  public MySQLDataNode(DataNodeConfig dataNodeConfig) {
    this.dataNodeConfig = dataNodeConfig;
  }

  public String getDatabaseName() {
    return dataNodeConfig.getDatabase();
  }


  public String getReplicaName() {
    return dataNodeConfig.getReplica();
  }

  public MycatReplica getReplica() {
    return replica;
  }

  public void setReplica(MycatReplica replica) {
    this.replica = replica;
  }

  @Override
  public String getName() {
    return dataNodeConfig.getName();
  }

  @Override
  public String getNodeID() {
    return getReplicaName();
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MySQLDataNode that = (MySQLDataNode) o;
    return Objects.equals(replica, that.replica) &&
               Objects.equals(dataNodeConfig, that.dataNodeConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(replica, dataNodeConfig);
  }
}
