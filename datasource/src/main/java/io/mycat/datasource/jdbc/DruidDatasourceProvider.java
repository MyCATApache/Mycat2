/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author Junwen Chen
 **/
public class DruidDatasourceProvider implements DatasourceProvider {

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

        DruidDataSource datasource = new DruidDataSource();
        datasource.setPassword(password);
        datasource.setUsername(username);
        datasource.setUrl(url);
        datasource.setMaxWait(TimeUnit.SECONDS.toMillis(60));
        datasource.setMaxActive(maxCon);
        datasource.setMinIdle(minCon);
        datasource.setKeepAlive(true);
        datasource.setTestOnReturn(true);
        datasource.setTestOnBorrow(true);
        datasource.setValidationQuery("select 'x'");
        datasource.setTestWhileIdle(true);

        if (maxRetryCount > 0) {
            datasource.setConnectionErrorRetryAttempts(maxRetryCount);
        }
        if (dbType != null) {
            datasource.setDbType(dbType);
        }
        if (initSQLs != null) {
            datasource.setConnectionInitSqls(initSQLs);
        }

        return new JdbcDataSource(config, datasource);
    }

    @Override
    public void closeDataSource(JdbcDataSource dataSource) {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Override
    public void init(ServerConfig config) {

    }
//
//    @Override
//    public TransactionSession createSession(MycatDataContext context) {
//        LocalTransactionSession localTransactionSession = new LocalTransactionSession(context);
//        context.setTransactionSession(localTransactionSession);
//        return localTransactionSession;
//    }
}