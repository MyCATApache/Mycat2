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

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.mysql.cj.jdbc.MysqlXADataSource;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import lombok.SneakyThrows;

import javax.transaction.UserTransaction;
import java.util.List;
import java.util.Properties;
/**
 * @author Junwen Chen
 **/
public class AtomikosDatasourceProvider implements DatasourceProvider {

  @SneakyThrows
  @Override
  public JdbcDataSource createDataSource(DatasourceRootConfig.DatasourceConfig config) {
    String username = config.getUser();
    String password = config.getPassword();
    String url = config.getUrl();
    String dbType = config.getDbType();
    int maxRetryCount = config.getMaxRetryCount();
    List<String> initSQL = config.getInitSqls();
    int maxCon = config.getMaxCon();
    int minCon = config.getMinCon();

    Properties p = new Properties();
    p.setProperty("pinGlobalTxToPhysicalConnection","true");
    p.setProperty("com.atomikos.icatch.serial_jta_transactions", "false");
    AtomikosDataSourceBean ds = new AtomikosDataSourceBean();
    ds.setXaProperties(p);
    ds.setConcurrentConnectionValidation(true);
    ds.setUniqueResourceName(config.getName());
    ds.setPoolSize(minCon);
    ds.setMaxPoolSize(maxCon);

    ds.setBorrowConnectionTimeout(60);
    ///////////////////////////////////////
    ds.setLocalTransactionMode(true);
    //////////////////////////////////////
    MysqlXADataSource mysqlXaDataSource = new MysqlXADataSource();
    mysqlXaDataSource.setUser(username);
    mysqlXaDataSource.setPassword(password);
    mysqlXaDataSource.setUrl(url);
    mysqlXaDataSource.setPinGlobalTxToPhysicalConnection(true);
    mysqlXaDataSource.setMaxReconnects(maxRetryCount);
//    DruidXADataSource   datasource = new DruidXADataSource();
//
//    if (maxRetryCount > 0) {
//      datasource.setConnectionErrorRetryAttempts(maxRetryCount);
//    }
//    if (dbType != null) {
//      datasource.setDbType(dbType);
//    }
//    if (initSQL != null) {
//      datasource.setConnectionInitSqls(
//          SQLParserUtils.createSQLStatementParser(initSQL, dbType).parseStatementList().stream()
//              .map(Object::toString).collect(
//              Collectors.toList()));
//    }
//    if (jdbcDriver != null) {
//      datasource.setD(jdbcDriver);
//    }
    ds.setXaDataSource(mysqlXaDataSource);
    return new JdbcDataSource(config,ds);
  }

  @Override
  public void closeDataSource(JdbcDataSource dataSource) {

  }

  @Override
  public UserTransaction createUserTransaction() {
    return new UserTransactionImp();
  }
}