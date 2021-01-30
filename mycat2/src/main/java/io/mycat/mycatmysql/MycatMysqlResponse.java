package io.mycat.mycatmysql;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.ExecuteType;
import io.mycat.ExplainDetail;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.mycat.vertx.VertxResponse;
import io.mycat.vertx.VertxSession;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;

public class MycatMysqlResponse extends VertxResponse {
    private final XaSqlConnection xAConnection;

    public MycatMysqlResponse(
                              int size,
                              boolean binary,
                              MycatMysqlSession mycatMysqlSession) {
        super(mycatMysqlSession, size, binary);
        this.xAConnection = mycatMysqlSession.getXaConnection();
    }

    @Override
    public PromiseInternal<Void> rollback() {
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        xAConnection.rollback(event -> {
            syncState();
            if (event.succeeded()) {
                sendOk().handle(promise);
            } else {
                sendError(event.cause()).handle(promise);
            }
        });
        return promise;
    }

    private void syncState() {
        dataContext.setAutoCommit(xAConnection.isAutocommit());
        dataContext.setInTransaction(xAConnection.isInTranscation());
    }

    @Override
    public PromiseInternal<Void> begin() {
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        xAConnection.begin(event -> {
            syncState();
            if (event.succeeded()) {
                sendOk().handle(promise);
            } else {
                sendError(event.cause()).handle(promise);
            }
        });
        return promise;
    }

    @Override
    public PromiseInternal<Void> commit() {
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        xAConnection.commit(event -> {
            syncState();
            if (event.succeeded()) {
                sendOk().handle(promise);
            } else {
                sendError(event.cause()).handle(promise);
            }
        });
        return promise;
    }


    @Override
    public PromiseInternal<Void> execute(ExplainDetail detail) {
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        String target = detail.getTarget();
        ExecuteType executeType = detail.getExecuteType();
        String sql = detail.getSql();
        MycatDataContext dataContext = session.getDataContext();

        switch (executeType) {
            case QUERY:
                target = dataContext.resolveDatasourceTargetName(target, false);
                break;
            case QUERY_MASTER:
            case INSERT:
            case UPDATE:
            default:
                target = dataContext.resolveDatasourceTargetName(target, true);
                break;
        }
        Future<SqlConnection> connection = xAConnection.getConnection(target);

        count++;
        switch (executeType) {
            case QUERY:
            case QUERY_MASTER:
                Future<RowObservable> rowObservableFuture = VertxExecuter.runQuery(connection, sql);
                Future<PromiseInternal<Void>> map = rowObservableFuture.map(this::sendResultSet);
                map.onSuccess(event -> promise.tryComplete()).onFailure(throwable -> promise.fail(throwable));
                break;
            case INSERT:
            case UPDATE:
                Future<long[]> future = VertxExecuter.runUpdate(connection, sql);
                future.onSuccess(event -> {
                    closeStatementState();
                    sendOk(event[0], event[1]);
                })
                        .onFailure(throwable -> {
                            closeStatementState();
                            promise.fail(throwable);
                        });
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + executeType);
        }
        return promise;
    }

    Future closeStatementState() {
        return CompositeFuture.all(xAConnection.closeStatementState(),
                Future.future((Handler<Promise<Void>>) event -> dataContext.getTransactionSession().closeStatenmentState()));

    }

    @Override
    public PromiseInternal<Void> sendResultSet(RowObservable rowIterable) {
        count++;
        PromiseInternal<Void> promise = VertxUtil.newSuccessPromise();
        rowIterable.subscribe(new ObserverWrite(new ObserverTask(rowIterable) {

            @Override
            public void onCloseResource() {
                dataContext.getTransactionSession().closeStatenmentState();
                xAConnection.closeStatementState();
                closeStatementState();
            }

            @Override
            public void onError(Throwable throwable) {
                promise.fail(throwable);
            }

            @Override
            public void onComplete() {
                promise.tryComplete();
            }
        }));
        return promise;
    }

    @Override
    public PromiseInternal<Void> sendResultSet(RowIterable rowIterable) {
        return getPromiseInternal(new IterableTask(rowIterable) {
            @Override
            public void onCloseResource() {
                dataContext.getTransactionSession().closeStatenmentState();
                xAConnection.closeStatementState();
            }
        });
    }
}
