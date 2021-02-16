package io.mycat.commands;

import io.mycat.*;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.proxy.session.MycatSession;
import io.mycat.runtime.ProxyTransactionSession;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.mycat.vertx.VertxResponse;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

import static io.mycat.ExecuteType.*;


public class ReceiverImpl implements Response {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverImpl.class);

    protected final MycatSession session;
    protected final MycatDataContext dataContext;
    protected final ProxyTransactionSession transactionSession;
    private final int stmtSize;
    private final boolean binary;
    protected int count = 0;

    public ReceiverImpl(MycatSession session, int stmtSize, boolean binary) {
        this.stmtSize = stmtSize;
        this.binary = binary;
        this.session = session;
        this.dataContext = this.session.getDataContext();
        this.transactionSession = (ProxyTransactionSession) this.dataContext.getTransactionSession();
    }


    @Override
    public PromiseInternal<Void> sendError(Throwable e) {
        session.setLastMessage(e);
        return VertxUtil.newFailPromise(new RuntimeException(e));
    }

    @Override
    public PromiseInternal<Void> proxySelect(String defaultTargetName, String statement) {
        return execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }


    @Override
    public PromiseInternal<Void> proxyUpdate(String defaultTargetName, String sql) {
        return execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(defaultTargetName), sql, null));
    }

    @Override
    public PromiseInternal<Void> proxySelectToPrototype(String statement) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return execute(ExplainDetail.create(QUERY_MASTER, Objects.requireNonNull(metadataManager.getPrototype()), statement, null));
    }


    @Override
    public PromiseInternal<Void> sendError(String errorMessage, int errorCode) {
        session.setLastMessage(errorMessage);
        session.setLastErrorCode(errorCode);
        return VertxUtil.newFailPromise(new MycatException(errorCode, errorMessage));
    }

    @Override
    public PromiseInternal<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
        count++;
        boolean hasMoreResult = hasMoreResultSet();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        mysqlPacketObservable.subscribe(
                new VertxResponse.MysqlPayloadObjectObserver(promise, hasMoreResult, binary, session));
        return promise;
    }


    @Override
    public PromiseInternal<Void> rollback() {
        count++;
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        class RollbackSendOk extends AsyncSendOk {
            public RollbackSendOk(PromiseInternal<Void> promise, boolean hasMoreResultSet) {
                super(promise, hasMoreResultSet);
            }
        }
        transactionSession.rollback()
                .onComplete(new RollbackSendOk(promise, hasMoreResultSet()));
        return promise;
    }

    @Override
    public PromiseInternal<Void> begin() {
        count++;
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        class BeginSendOk extends AsyncSendOk {
            public BeginSendOk(PromiseInternal<Void> promise, boolean hasMoreResultSet) {
                super(promise, hasMoreResultSet);
            }
        }
        transactionSession.begin().onComplete(new BeginSendOk(promise, hasMoreResultSet()));
        return promise;
    }

    @Override
    public PromiseInternal<Void> commit() {
        count++;
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        class CommitSendOk extends AsyncSendOk {
            public CommitSendOk(PromiseInternal<Void> promise, boolean hasMoreResultSet) {
                super(promise, hasMoreResultSet);
            }
        }
        transactionSession.commit()
                .onComplete(new CommitSendOk(promise, hasMoreResultSet()));
        return promise;
    }

    @Override
    public PromiseInternal<Void> execute(ExplainDetail detail) {
        boolean directPacket = false;
        boolean master = session.isInTransaction() || !session.isAutocommit() || detail.getExecuteType().isMaster();
        String datasource = session.getDataContext().resolveDatasourceTargetName(detail.getTarget(), master);
        String sql = detail.getSql();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        boolean inTransaction = session.isInTransaction();
        Future<SqlConnection> connectionFuture = transactionSession.getConnection(datasource);
        switch (detail.getExecuteType()) {
            case QUERY:
            case QUERY_MASTER: {
                Future<Void> future = connectionFuture.flatMap(connection -> {
                    Observable<MysqlPayloadObject> mysqlPacketObservable = VertxExecuter.runQueryOutputAsMysqlPayloadObject(Future.succeededFuture(
                            connection), sql, Collections.emptyList());
                    if (!inTransaction) {
                        return sendResultSet(mysqlPacketObservable);
                    } else {
                        return sendResultSet(mysqlPacketObservable);
                    }
                });
                future.onComplete(event -> {
                    if (event.succeeded()) {
                        promise.tryComplete();
                    } else {
                        promise.tryFail(event.cause());
                    }
                });
                return promise;
            }
            case UPDATE:
            case INSERT:
                count++;
                Future<long[]> future1 = VertxExecuter.runUpdate(connectionFuture, sql);
                future1.onComplete(event -> {
                    if (event.succeeded()) {
                        long[] result = event.result();
                        sendOk(result[0], result[1]).onComplete(result1 -> promise.tryComplete());
                    } else {
                        promise.tryFail(event.cause());
                    }

                });
                return promise;
            default:
                throw new IllegalStateException("Unexpected value: " + detail.getExecuteType());
        }
    }

    @Override
    public PromiseInternal<Void> sendOk() {
        count++;
        PromiseInternal<Void> promise = VertxUtil.newSuccessPromise();
        new AsyncSendOk(promise, hasMoreResultSet()).handle(VertxUtil.newSuccessPromise());
        return promise;
    }

    @Override
    public PromiseInternal<Void> sendOk(long affectedRow, long lastInsertId) {
        session.setLastInsertId(lastInsertId);
        session.setAffectedRows(affectedRow);
        return sendOk();
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }

//    @Override
//    public PromiseInternal<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
//        count++;
//        boolean hasMoreResultSet = hasMoreResultSet();
//        PromiseInternal<Void> voidPromiseInternal = VertxUtil.newPromise();
//        mysqlPacketObservable.subscribe(
//                new VertxResponse.MysqlPayloadObjectObserver(voidPromiseInternal,hasMoreResultSet,binary,session));
//        return voidPromiseInternal;
//    }

    protected boolean hasMoreResultSet() {
        return count < this.stmtSize;
    }

    private class AsyncSendOk implements Handler<AsyncResult<Void>> {
        private final PromiseInternal<Void> promise;
        private final boolean hasMoreResultSet;

        public AsyncSendOk(PromiseInternal<Void> promise, boolean hasMoreResultSet) {
            this.promise = promise;
            this.hasMoreResultSet = hasMoreResultSet;
        }

        @Override
        public void handle(AsyncResult<Void> event) {
            transactionSession.closeStatementState().onComplete(unused -> {
                if (!event.succeeded()) {
                    promise.tryFail(event.cause());
                } else {
                    session.writeOk(hasMoreResultSet)
                            .onComplete(event1 -> promise.tryComplete());
                }
            });
        }
    }
}