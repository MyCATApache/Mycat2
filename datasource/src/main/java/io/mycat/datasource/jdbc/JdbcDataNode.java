package io.mycat.datasource.jdbc;

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