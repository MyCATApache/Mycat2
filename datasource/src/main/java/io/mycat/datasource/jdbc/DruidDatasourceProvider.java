/**
 * Copyright (C) <2021>  <chen junwen>
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

import com.alibaba.druid.DbType;
import com.alibaba.druid.pool.DruidDataSource;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author Junwen Chen
 **/
public class DruidDatasourceProvider implements DatasourceProvider {

    @Override
    public JdbcDataSource createDataSource(DatasourceConfig config) {
        if (System.getProperty("druid.mysql.usePingMethod") == null) {
            System.setProperty("druid.mysql.usePingMethod", "false");
        }
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
        datasource.setMaxWait(config.getMaxConnectTimeout());
        datasource.setMaxActive(maxCon);
        datasource.setMinIdle(minCon);
        datasource.setKeepAlive(true);
//        datasource.setTestOnReturn(true);
//        datasource.setTestOnBorrow(true);
        datasource.setValidationQuery("select 'x'");
        datasource.setTestWhileIdle(true);
        datasource.setQueryTimeout(config.getQueryTimeout());

        if(config.isRemoveAbandoned()){
            datasource.setRemoveAbandoned(true);
            datasource.setRemoveAbandonedTimeout(config.getRemoveAbandonedTimeoutSecond());
            datasource.setLogAbandoned(config.isLogAbandoned());
        }

        if (maxRetryCount > 0) {
            datasource.setConnectionErrorRetryAttempts(maxRetryCount);
        }
        if (dbType != null) {
            datasource.setDbType(dbType);
        }
        if (initSQLs != null) {
            datasource.setConnectionInitSqls(initSQLs);
        }
        DataSource finalDataSource;
        if (config.computeType().isJdbc() && !"mysql".equalsIgnoreCase(config.getDbType())) {
            dbType = Optional.ofNullable(dbType).orElse("mysql");
            DbType dbTypeEnum = DbType.of(dbType);
            SQLDialect dialect;
            switch (dbTypeEnum) {
                case jtds:
                case other:
                case db2:
                case oracle:
                case hive:
                case dm:
                case polardb:
                case sqlserver:
                default:
                    dialect = SQLDialect.DEFAULT;
                    break;
                case hsql:
                    dialect = SQLDialect.HSQLDB;
                    break;
                case postgresql:
                    dialect = SQLDialect.POSTGRES;
                    break;
                case oceanbase:
                case mysql:
                    dialect = SQLDialect.MYSQL;
                    break;
                case mariadb:
                    dialect = SQLDialect.MARIADB;
                    break;
                case derby:
                    dialect = SQLDialect.DERBY;
                    break;
                case h2:
                    dialect = SQLDialect.H2;
                    break;
                case clickhouse:
                    return new JdbcDataSource(config, datasource);
            }
            DSLContext using = DSL.using(datasource, dialect);
            finalDataSource = using.parsingDataSource();
        } else {
            finalDataSource = datasource;
        }
        return new JdbcDataSource(config, finalDataSource);
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
