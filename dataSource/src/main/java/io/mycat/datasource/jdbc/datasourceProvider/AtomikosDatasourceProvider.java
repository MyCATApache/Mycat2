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

import com.alibaba.druid.pool.xa.DruidXADataSource;
import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

public class AtomikosDatasourceProvider implements DatasourceProvider {

  @Override
  public DataSource createDataSource(JdbcDataSource config, Map<String, String> jdbcDriverMap) {
    String password = config.getPassword();
    String username = config.getUsername();
    String url = config.getUrl();
    String dbType = config.getDbType();
    String db = config.getDb();
    String jdbcDriver = jdbcDriverMap.get(dbType);
    String datasourceName = config.getName();

    Properties p = new Properties();
    p.setProperty("com.atomikos.icatch.serial_jta_transactions", "false");
    AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
    ds.setXaProperties(p);
    ds.setConcurrentConnectionValidation(true);
    ds.setUniqueResourceName(datasourceName);
    ds.setPoolSize(1);
    ds.setMaxPoolSize(1000);
    ds.setLocalTransactionMode(true);
    ds.setBorrowConnectionTimeout(60);
    ds.setReapTimeout(1000);
    ds.setMaxLifetime(1000);
//
//    MysqlXADataSource mysqlXaDataSource = new MysqlXADataSource();
//    mysqlXaDataSource.setURL(url);
//    mysqlXaDataSource.setUser(username);
//    mysqlXaDataSource.setPassword(password);

    DruidXADataSource datasource = new DruidXADataSource();
    datasource.setPassword(password);
    datasource.setUsername(username);
    datasource.setUrl(url);
    datasource.setMaxActive(1000);
    datasource.setMaxWait(TimeUnit.SECONDS.toMillis(5));
    try {
//      mysqlXaDataSource.setConnectTimeout(10000);
//      mysqlXaDataSource.setAutoReconnectForPools(true);
//      mysqlXaDataSource.setPinGlobalTxToPhysicalConnection(true);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // ds.setXaDataSource(mysqlXaDataSource);
    ds.setXaDataSource(datasource);
    return datasource;
  }

  @Override
  public boolean isJTA() {
    return true;
  }

  @Override
  public UserTransaction createUserTransaction() {
    return new UserTransactionImp();
  }
}