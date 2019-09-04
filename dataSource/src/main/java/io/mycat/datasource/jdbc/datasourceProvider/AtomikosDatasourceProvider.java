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
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

public class AtomikosDatasourceProvider implements DatasourceProvider {

  @Override
  public DataSource createDataSource(JdbcDataSource jdbcDataSource) {
    DatasourceConfig config = jdbcDataSource.getConfig();
    String username = config.getUser();
    String password = config.getPassword();
    String url = config.getUrl();
    String dbType = config.getDbType();
    String initDb = config.getInitDb();
    int maxRetryCount = config.getMaxRetryCount();
    String initSQL = config.getInitSQL();

    String jdbcDriver = config.getJdbcDriverClass();
    int maxCon = config.getMaxCon();
    int minCon = config.getMinCon();

    Properties p = new Properties();
    p.setProperty("com.atomikos.icatch.serial_jta_transactions", "false");
    AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
    ds.setXaProperties(p);
    ds.setConcurrentConnectionValidation(true);
    ds.setUniqueResourceName(jdbcDataSource.getName());
    ds.setPoolSize(minCon);
    ds.setMaxPoolSize(maxCon);
    ds.setLocalTransactionMode(true);
    ds.setBorrowConnectionTimeout(60);

    DruidXADataSource datasource = new DruidXADataSource();
    datasource.setPassword(password);
    datasource.setUsername(username);
    datasource.setUrl(url);
    datasource.setMaxWait(TimeUnit.SECONDS.toMillis(1));
    datasource.setMaxActive(maxCon);
    datasource.setMinIdle(minCon);

    if (maxRetryCount > 0) {
      datasource.setConnectionErrorRetryAttempts(maxRetryCount);
    }
    if (dbType != null) {
      datasource.setDbType(dbType);
    }
    if (initSQL != null) {
      datasource.setConnectionInitSqls(
          SQLParserUtils.createSQLStatementParser(initSQL, dbType).parseStatementList().stream()
              .map(Object::toString).collect(
              Collectors.toList()));
    }
    if (initDb != null) {

    }
    if (jdbcDriver != null) {
      datasource.setDriverClassName(jdbcDriver);
    }
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