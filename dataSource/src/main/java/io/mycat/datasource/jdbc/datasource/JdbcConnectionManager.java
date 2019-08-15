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


import io.mycat.MycatException;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class JdbcConnectionManager implements ConnectionManager {

  private final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(JdbcConnectionManager.class);
  private final HashMap<JdbcDataSource, DataSource> dataSourceMap = new HashMap<>();
  private final DatasourceProvider datasourceProvider;
  private final List<JdbcDataSource> dataSources;


  public JdbcConnectionManager(GRuntime runtime,
      DatasourceProvider provider, Map<String, String> jdbcDriverMap,
      List<JdbcDataSource> dataSources) {
    Objects.requireNonNull(jdbcDriverMap);
    Objects.requireNonNull(runtime);
    Objects.requireNonNull(provider);
    Objects.requireNonNull(dataSources);
    this.datasourceProvider = provider;
    for (JdbcDataSource dataSource : dataSources) {
      DataSource pool = datasourceProvider
          .createDataSource(dataSource, jdbcDriverMap);
      dataSourceMap.put(dataSource, pool);
    }
    this.dataSources = dataSources;
  }

  private DataSource getPool(JdbcDataSource datasource) {
    return dataSourceMap.get(datasource);
  }


  public Connection getConnection(JdbcDataSource key) {
    if (key.counter.updateAndGet(operand -> {
      if (operand < key.getMaxCon()) {
        return ++operand;
      }
      return operand;
    }) < key.getMaxCon()) {
      DataSource pool = getPool(key);
      try {
        return pool.getConnection();
      } catch (SQLException e) {
        throw new MycatException(e);
      }
    } else {
      throw new MycatException("max limit");
    }
  }

  @Override
  public void closeConnection(JdbcDataSource key, Connection connection) {
    key.counter.updateAndGet(operand -> {
      if (operand == 0) {
        return 0;
      }
      return --operand;
    });
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error("", e);
    }
  }

  public List<JdbcDataSource> getDatasourceList() {
    return dataSources;
  }

  public boolean isJTA() {
    return datasourceProvider.isJTA();
  }
}
