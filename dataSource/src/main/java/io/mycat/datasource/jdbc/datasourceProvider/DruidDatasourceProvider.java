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
package io.mycat.datasource.jdbc.datasourceProvider;

import com.alibaba.druid.pool.DruidDataSource;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import java.util.Map;
import javax.sql.DataSource;

public class DruidDatasourceProvider implements DatasourceProvider {

  @Override
  public DataSource createDataSource(JdbcDataSource config, Map<String, String> jdbcDriverMap) {
    String username = config.getUsername();
    String password = config.getPassword();
    String url = config.getUrl();
    String dbType = config.getDbType();
    String db = config.getDb();
    String jdbcDriver = jdbcDriverMap.get(dbType);

    DruidDataSource datasource = new DruidDataSource();
    datasource.setPassword(password);
    datasource.setUsername(username);
    datasource.setUrl(url);
    datasource.setMaxWait(5000);
    datasource.setMaxActive(200);
//    datasource.setDriverClassName(jdbcDriver);
    return datasource;
  }
}