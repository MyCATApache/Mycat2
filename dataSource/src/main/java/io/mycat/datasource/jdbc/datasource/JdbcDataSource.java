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

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.MycatConfigUtil;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class JdbcDataSource implements MycatDataSource {

  private final int index;
  private final DatasourceConfig datasourceConfig;
  private final JdbcReplica replica;
  private final boolean isMySQLType;
  final AtomicInteger counter = new AtomicInteger(0);
  final PhysicsInstance instance;

  public JdbcDataSource(int index, DatasourceConfig datasourceConfig,
      JdbcReplica replica) {
    this.index = index;
    this.datasourceConfig = datasourceConfig;
    this.replica = replica;
    String dbType = datasourceConfig.getDbType();
    this.isMySQLType = MycatConfigUtil.isMySQLType(datasourceConfig);
    this.instance = ReplicaSelectorRuntime.INSTCANE
        .registerDatasource(replica.getName(), datasourceConfig,
            index, () -> counter.get());
  }

  public String getUrl() {
    return datasourceConfig.getUrl();
  }

  public String getUsername() {
    return datasourceConfig.getUser();
  }

  public String getPassword() {
    return datasourceConfig.getPassword();
  }

  public String getName() {
    return datasourceConfig.getName();
  }

  public int getIndex() {
    return index;
  }

  public boolean isMySQLType() {
    return isMySQLType;
  }

  public String getDb() {
    return datasourceConfig.getInitDb();
  }

  public JdbcReplica getReplica() {
    return replica;
  }

  public int getMaxCon() {
    return datasourceConfig.getMaxCon();
  }

  public String getDbType() {
    return datasourceConfig.getDbType();
  }

  public PhysicsInstance instance() {
    return instance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JdbcDataSource that = (JdbcDataSource) o;

    if (index != that.index) {
      return false;
    }
    return datasourceConfig != null ? datasourceConfig.equals(that.datasourceConfig)
        : that.datasourceConfig == null;
  }

  @Override
  public int hashCode() {
    int result = index;
    result = 31 * result + (datasourceConfig != null ? datasourceConfig.hashCode() : 0);
    return result;
  }

  public DatasourceConfig getConfig() {
    return datasourceConfig;
  }
}
