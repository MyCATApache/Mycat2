package io.mycat.vertx;

import io.mycat.*;
import io.mycat.api.collector.*;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.util.VertxUtil;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import static io.mycat.ExecuteType.QUERY;
import static io.mycat.ExecuteType.UPDATE;

public abstract class VertxResponse implements Response {

    protected final MycatDataContext dataContext;
    protected VertxSession session;
    protected final int size;
    protected int count;
    protected boolean binary;

    public VertxResponse(VertxSession session, int size, boolean binary) {
        this.session = session;
        this.size = size;
        this.binary = binary;
        this.dataContext = session.getDataContext();
    }

    @Override
    public PromiseInternal<Void> proxySelect(String defaultTargetName, String statement) {
        return execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }

    @Override
    public PromiseInternal<Void> proxyUpdate(String defaultTargetName, String proxyUpdate) {
        return execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(defaultTargetName), proxyUpdate, null));
    }

    @Override
    public PromiseInternal<Void> sendError(Throwable e) {
        PromiseInternal<Void> newPromise = VertxUtil.newPromise();
        dataContext.getTransactionSession().closeStatementState()
                .onComplete(event -> {
                    dataContext.setLastMessage(e);
                    newPromise.tryComplete();
                    session.writeErrorEndPacketBySyncInProcessError();
                });
        return newPromise;
    }

    @Override
    public PromiseInternal<Void> proxySelectToPrototype(String statement) {
        return proxySelect("prototype", statement);
    }


    @Override
    public PromiseInternal<Void> sendError(String errorMessage, int errorCode) {
        dataContext.getTransactionSession().closeStatementState();
        dataContext.setLastMessage(errorMessage);
        return VertxUtil.newFailPromise(new MycatException(errorCode,errorMessage));
    };


    @Override
    public PromiseInternal<Void> execute(ExplainDetail detail) {
        PromiseInternal<Void> promise;
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
        TransactionSession transactionSession = dataContext.getTransactionSession();
        MycatConnection connection = transactionSession.getJDBCConnection(target);
        count++;
        switch (executeType) {
            case QUERY:
            case QUERY_MASTER:
                promise = sendResultSet(connection.executeQuery(null, sql));
                break;
            case INSERT:
            case UPDATE:
                long[] longs = connection.executeUpdate(sql, true);
                transactionSession.closeStatementState();
                promise = sendOk(longs[0], longs[1]);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + executeType);
        }
        return promise;
    }

    @Override
    public PromiseInternal<Void> sendOk(long affectedRow, long lastInsertId) {
        count++;
        MycatDataContext dataContext = session.getDataContext();
        dataContext.getTransactionSession().closeStatementState();
        dataContext.setLastInsertId(lastInsertId);
        dataContext.setAffectedRows(affectedRow);
        return session.writeOk(count < size);
    }

    @Override
    public PromiseInternal<Void> sendOk() {
        count++;
        PromiseInternal<Void> newPromise = VertxUtil.newPromise();
        MycatDataContext dataContext = session.getDataContext();
        dataContext.getTransactionSession().closeStatementState()
                .onComplete(event -> {
                    newPromise.tryComplete();
                    session.writeOk(count < size);
                });
        return newPromise;
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }

    @Override
    public PromiseInternal<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
        count++;
        boolean moreResultSet = count < size;
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        mysqlPacketObservable.subscribe(
                new MysqlPayloadObjectObserver(promise, moreResultSet,binary,session));
        return promise;
    }

    public   static class MysqlPayloadObjectObserver implements Observer<MysqlPayloadObject> {
        private final PromiseInternal<Void> promise;
        private final boolean moreResultSet;
        private boolean binary;
        private MySQLServerSession session;
        private Disposable disposable;
        Function<Object[], byte[]> convertor;

        public MysqlPayloadObjectObserver(PromiseInternal<Void> promise,
                                          boolean moreResultSet,boolean binary, MySQLServerSession session) {
            this.promise = promise;
            this.moreResultSet = moreResultSet;
            this.binary = binary;
            this.session = session;
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            this.disposable = d;
        }

        @Override
        public void onNext(@NonNull MysqlPayloadObject next) {
            if (next instanceof MysqlRow) {
                session.writeBytes(this.convertor.apply(((MysqlRow) next).getRow()), false);
            } else if (next instanceof MySQLColumnDef) {
                MycatRowMetaData rowMetaData = ((MySQLColumnDef) next).getMetaData();
                session.writeColumnCount(rowMetaData.getColumnCount());
                if (!binary) {
                    convertor = ResultSetMapping.concertToDirectTextResultSet(rowMetaData);
                } else {
                    convertor = ResultSetMapping.concertToDirectBinaryResultSet(rowMetaData);
                }
                Iterator<byte[]> columnIterator = MySQLPacketUtil.generateAllColumnDefPayload(rowMetaData).iterator();
                while (columnIterator.hasNext()) {
                    session.writeBytes(columnIterator.next(), false);
                }
                session.writeColumnEndPacket(moreResultSet);
            }
        }

        @Override
        public void onError(@NonNull Throwable throwable) {
            disposable.dispose();
            promise.tryFail(throwable);
        }

        @Override
        public void onComplete() {
            disposable.dispose();
            session.getDataContext().getTransactionSession().closeStatementState()
                    .onComplete(event -> {
                        session.writeRowEndPacket(moreResultSet, false)
                        .onComplete((Handler<AsyncResult>) event1 -> promise.tryComplete());
                    });
        }
    }
}