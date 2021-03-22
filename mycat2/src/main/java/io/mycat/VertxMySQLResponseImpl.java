package io.mycat;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.sqlhandler.VertxExecuter;
import io.mycat.vertx.VertxSession;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.SqlConnection;

import java.util.Collections;
import java.util.List;

public class VertxMySQLResponseImpl implements Response {
    final XaSqlConnection xaSqlConnection = new;
    final VertxSession session;
    @Override
    public void sendError(Throwable e) {
        session.writeBytes();
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        Future<SqlConnection> connection = xaSqlConnection.getConnection(defaultTargetName);
        Future<RowObservable> observableFuture = VertxExecuter.runQuery(connection, statement);
        observableFuture.onSuccess(event -> sendResultSet(event,event.getRowMetaData()))
                .onFailure(event -> sendError(event));
    }

    @Override
    public void proxyUpdate(String defaultTargetName, String proxyUpdate) {
        Future<SqlConnection> connection = xaSqlConnection.getConnection(defaultTargetName);
        Future<long[]> future = VertxExecuter.runUpdate(proxyUpdate, connection);
        future.onSuccess(event -> sendOk(event[0],event[1]))
                .onFailure(event -> sendError(event));
    }

    @Override
    public void proxySelectToPrototype(String statement) {
        proxySelect("prototype",statement);

    }

    @Override
    public void sendError(String errorMessage, int errorCode) {

    }

    @Override
    public void sendResultSet(Observable<Object[]> rowIterable, MycatRowMetaData mycatRowMetaData, ResultType resultType) {

    }

    @Override
    public void sendResultSet(RowIterable rowIterable) {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void begin() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void execute(ExplainDetail detail) {

    }

    @Override
    public void sendOk() {

    }

    @Override
    public void sendOk(long affectedRow, long lastInsertId) {

    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }
}
