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
package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.config.schema.DataNodeConfig;

public class JdbcDataNode extends MycatDataNode {

  public final JdbcReplica replica;
  private final DataNodeConfig dataNodeConfig;

  public JdbcDataNode(JdbcReplica replica, DataNodeConfig dataNodeConfig) {
    this.replica = replica;
    this.dataNodeConfig = dataNodeConfig;
  }

  @Override
  public String getName() {
    return dataNodeConfig.getName();
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JdbcDataNode that = (JdbcDataNode) o;

    if (replica != null ? !replica.equals(that.replica) : that.replica != null) {
      return false;
    }
    return dataNodeConfig != null ? dataNodeConfig.equals(that.dataNodeConfig)
        : that.dataNodeConfig == null;
  }

  @Override
  public int hashCode() {
    int result = replica != null ? replica.hashCode() : 0;
    result = 31 * result + (dataNodeConfig != null ? dataNodeConfig.hashCode() : 0);
    return result;
  }

  public JdbcReplica getReplica() {
    return replica;
  }

  public String getDatabase() {
    return dataNodeConfig.getDatabase();
  }
}