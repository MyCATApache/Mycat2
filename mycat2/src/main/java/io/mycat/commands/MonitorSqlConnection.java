package io.mycat.commands;

import io.mycat.beans.log.monitor.DatabaseInstanceEntry;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.spi.DatabaseMetadata;

public class MonitorSqlConnection implements SqlConnection {
    final SqlConnection sqlConnection;
    private DatabaseInstanceEntry stat;
    final private MonitorMycatDatasourcePool monitorMycatDatasourcePool2;

    public MonitorSqlConnection(SqlConnection sqlConnection, DatabaseInstanceEntry stat, MonitorMycatDatasourcePool monitorMycatDatasourcePool2) {
        this.sqlConnection = sqlConnection;
        this.stat = stat;
        this.monitorMycatDatasourcePool2 = monitorMycatDatasourcePool2;
    }

    @Override
    public SqlConnection prepare(String s, Handler<AsyncResult<PreparedStatement>> handler) {
        return sqlConnection.prepare(s, handler);
    }

    @Override
    public Future<PreparedStatement> prepare(String s) {
        return sqlConnection.prepare(s);
    }

    @Override
    public SqlConnection exceptionHandler(Handler<Throwable> handler) {
        return sqlConnection.exceptionHandler(handler);
    }

    @Override
    public SqlConnection closeHandler(Handler<Void> handler) {
        return sqlConnection.closeHandler(handler);
    }

    @Override
    public void begin(Handler<AsyncResult<Transaction>> handler) {
        sqlConnection.begin(handler);
    }

    @Override
    public Future<Transaction> begin() {
        return sqlConnection.begin();
    }

    @Override
    public boolean isSSL() {
        return sqlConnection.isSSL();
    }

    @Override
    public Query<RowSet<Row>> query(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
        return sqlConnection.preparedQuery(s);
    }

    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
        sqlConnection.close().onComplete(handler);
    }

    @Override
    public Future<Void> close() {
        return sqlConnection.close().onComplete(event -> {
            stat.decCon();
        });
    }

    @Override
    public DatabaseMetadata databaseMetadata() {
        return sqlConnection.databaseMetadata();
    }

}
