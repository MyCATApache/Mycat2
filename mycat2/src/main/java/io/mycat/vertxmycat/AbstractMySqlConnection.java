package io.mycat.vertxmycat;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.mysqlclient.MySQLAuthOptions;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.mysqlclient.MySQLSetOption;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.spi.DatabaseMetadata;

public abstract class AbstractMySqlConnection implements MySQLConnection {

    @Override
    public MySQLConnection prepare(String sql, Handler<AsyncResult<PreparedStatement>> handler) {
        Future<PreparedStatement> fut = prepare(sql);
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }


    @Override
    public void begin(Handler<AsyncResult<Transaction>> handler) {
        Future<Transaction> fut = begin();
        if (handler != null) {
            fut.onComplete(handler);
        }
    }

    @Override
    public Future<Transaction> begin() {
       throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSSL() {
        return false;
    }


    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = close();
        if (handler != null) {
            fut.onComplete(handler);
        }
    }


    @Override
    public DatabaseMetadata databaseMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MySQLConnection ping(Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = ping();
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }

    @Override
    public MySQLConnection specifySchema(String schemaName, Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = specifySchema(schemaName);
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }


    @Override
    public MySQLConnection getInternalStatistics(Handler<AsyncResult<String>> handler) {
        Future<String> fut = getInternalStatistics();
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }


    @Override
    public MySQLConnection setOption(MySQLSetOption option, Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = setOption(option);
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }
    @Override
    public Future<String> getInternalStatistics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Void> setOption(MySQLSetOption option) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Void> debug() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Void> changeUser(MySQLAuthOptions options) {
        throw new UnsupportedOperationException();
    }


    @Override
    public MySQLConnection resetConnection(Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = resetConnection();
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }


    @Override
    public MySQLConnection debug(Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = debug();
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }


    @Override
    public MySQLConnection changeUser(MySQLAuthOptions options, Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = changeUser(options);
        if (handler != null) {
            fut.onComplete(handler);
        }
        return this;
    }

}
