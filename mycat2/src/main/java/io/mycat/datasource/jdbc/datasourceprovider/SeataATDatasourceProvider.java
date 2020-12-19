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
package io.mycat.datasource.jdbc.datasourceprovider;

import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.runtime.SeataTransactionSession;
import io.seata.rm.RMClient;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.tm.TMClient;

/**
 * @author Junwen Chen
 **/
public class SeataATDatasourceProvider extends DruidDatasourceProvider {

  @Override
  public JdbcDataSource createDataSource(DatasourceConfig config) {
    JdbcDataSource dataSource = super.createDataSource(config);
    return new JdbcDataSource(config,new DataSourceProxy(dataSource.getDataSource()));
  }

  @Override
  public TransactionSession createSession(MycatDataContext context) {
    return new SeataTransactionSession(context);
  }

  @Override
  public void init(ServerConfig config) {
    super.init(config);
    String appId = "api";
    String txGroup = "my_test_tx_group";
    TMClient.init(appId, txGroup);
    RMClient.init(appId, txGroup);
  }
}