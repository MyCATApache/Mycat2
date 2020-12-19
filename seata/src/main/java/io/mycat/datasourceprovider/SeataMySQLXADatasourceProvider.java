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
package io.mycat.datasourceprovider;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.xa.DruidXADataSource;
import com.mysql.cj.jdbc.MysqlXADataSource;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.rm.datasource.xa.DataSourceProxyXA;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Junwen Chen
 **/
public class SeataMySQLXADatasourceProvider extends SeataATDatasourceProvider {

  @Override
  public JdbcDataSource createDataSource(DatasourceConfig config) {
    String username = config.getUser();
    String password = config.getPassword();
    String url = Objects.requireNonNull(config.getUrl());
    String dbType = config.getDbType();
    int maxRetryCount = config.getMaxRetryCount();
    List<String> initSQLs = config.getInitSqls();

    int maxCon = config.getMaxCon();
    int minCon = config.getMinCon();

    MysqlXADataSource datasource = new MysqlXADataSource();
    datasource.setPassword(password);
    datasource.setUser(username);
    datasource.setUrl(url);

    return new JdbcDataSource(config,new DataSourceProxyXA(datasource));
  }

}