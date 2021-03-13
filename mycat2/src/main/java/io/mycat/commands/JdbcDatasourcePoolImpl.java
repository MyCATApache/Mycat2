package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.vertxmycat.JdbcMySqlConnection;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

public class JdbcDatasourcePoolImpl extends MycatDatasourcePool {
    public JdbcDatasourcePoolImpl(String targetName) {
        super(targetName);
    }

    @Override
    public Future<SqlConnection> getConnection() {
        try {
        return Future.succeededFuture(new JdbcMySqlConnection(targetName));
        } catch (Throwable throwable){
            return Future.failedFuture(throwable);
        }
    }

    @Override
    public Integer getAvailableNumber() {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            JdbcDataSource jdbcDataSource = jdbcConnectionManager.getDatasourceInfo().get(targetName);
            return jdbcDataSource.getMaxCon() - jdbcDataSource.getUsedCount();
    }

    @Override
    public Integer getUsedNumber() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        JdbcDataSource jdbcDataSource = jdbcConnectionManager.getDatasourceInfo().get(targetName);
        return jdbcDataSource.getUsedCount();
    }
}
