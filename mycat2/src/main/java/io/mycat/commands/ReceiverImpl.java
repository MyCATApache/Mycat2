package io.mycat.commands;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.resultset.MycatProxyResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.calcite.ProxyConnectionUsage;
import io.mycat.proxy.session.MycatSession;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.DirectTextResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.runtime.ProxyTransactionSession;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.ResultSetMapping;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

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
        return VertxUtil.newFailPromise(new MycatException(errorCode,errorMessage));
    }

    @Override
    public PromiseInternal<Void> sendResultSet(RowIterable rowIterable) {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        Future<Void> future = Future.future(event -> {
            RowBaseIterator resultSet = rowIterable.get();
            try {
                MycatResultSetResponse currentResultSet;
                if (!binary) {
                    if (resultSet instanceof JdbcRowBaseIterator) {
                        currentResultSet = new DirectTextResultSetResponse((resultSet));
                    } else {
                        currentResultSet = new TextResultSetResponse(resultSet);
                    }
                } else {
                    currentResultSet = new BinaryResultSetResponse(resultSet);
                }
                session.writeColumnCount(currentResultSet.columnCount());
                Iterator<byte[]> columnDefPayloadsIterator = currentResultSet
                        .columnDefIterator();
                while (columnDefPayloadsIterator.hasNext()) {
                    session.writeBytes(columnDefPayloadsIterator.next(), false);
                }
                session.writeColumnEndPacket();
                Iterator<byte[]> rowIterator = currentResultSet.rowIterator();
                while (rowIterator.hasNext()) {
                    byte[] row = rowIterator.next();
                    session.writeBytes(row, false);
                }

            } catch (Throwable throwable) {
                event.tryFail(throwable);
            } finally {
                resultSet.close();
            }
            event.tryComplete();
        });
        future.flatMap(unused -> session.getDataContext().getTransactionSession().closeStatenmentState()
                .flatMap(unused2 -> session.writeRowEndPacket(hasMoreResultSet, false)
                        .onComplete(event -> promise.tryComplete())))
                .recover(throwable -> session.getDataContext().getTransactionSession().closeStatenmentState()
                        .flatMap(unused3 -> {
                            ;
                            return sendError(throwable);
                        }).onComplete(event -> promise.tryFail(throwable)));
        return promise;
    }


    @Override
    public PromiseInternal<Void> rollback() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        Future<Void> commit = transactionSession.rollback();
        commit.onComplete(event -> transactionSession.closeStatenmentState().onComplete(unused -> {
            if (!event.succeeded()) {
                promise.tryFail(event.cause());
            } else {
                session.writeOk(hasMoreResultSet)
                        .onComplete(event1 -> promise.tryComplete());
            }
        }));
        return promise;
    }

    @Override
    public PromiseInternal<Void> begin() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        Future<Void> commit = transactionSession.begin();
        commit.onComplete(event -> transactionSession.closeStatenmentState().onComplete(unused -> {
            if (!event.succeeded()) {
                promise.tryFail(event.cause());
            } else {
                session.writeOk(hasMoreResultSet)
                        .onComplete(event1 -> promise.tryComplete());
            }
        }));
        return promise;
    }

    @Override
    public PromiseInternal<Void> commit() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        Future<Void> commit = transactionSession.commit();
        commit.onComplete(event -> transactionSession.closeStatenmentState().onComplete(unused -> {
            if (!event.succeeded()) {
                promise.tryFail(event.cause());
            } else {
                session.writeOk(hasMoreResultSet)
                        .onComplete(event1 -> promise.tryComplete());
            }
        }));
        return promise;
    }

    @Override
    public PromiseInternal<Void> execute(ExplainDetail detail) {
        count++;
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
                    RowObservable rowObservable = VertxExecuter.runQuery(Future.succeededFuture(
                            connection), sql);
                    if (!inTransaction) {
                        return sendResultSet(ProxyConnectionUsage.wrapAsAutoCloseConnectionRowObservale(
                                connection, rowObservable));
                    } else {
                        return sendResultSet(rowObservable);
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
                Future<long[]> future1 = VertxExecuter.runUpdate(connectionFuture, sql);
                future1.onComplete(event -> {
                    if (event.succeeded()){
                        long[] result = event.result();
                        sendOk(result[0],result[1]).onComplete(result1 -> promise.tryComplete());
                    }else {
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
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        transactionSession.closeStatenmentState().onComplete(event -> {
            session.writeOk(hasMoreResultSet()).onComplete(event1 -> {
                if (event1.succeeded()) {
                    promise.tryComplete();
                } else {
                    promise.tryFail(event1.cause());
                }
            });
        });
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

    @Override
    public PromiseInternal<Void> sendResultSet(RowObservable rowIterable) {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        PromiseInternal<Void> voidPromiseInternal = VertxUtil.newSuccessPromise();
        ObserverWrite observerWrite = new ObserverWrite(rowIterable, hasMoreResultSet, voidPromiseInternal);
        rowIterable.subscribe(observerWrite);
        return voidPromiseInternal;
    }

    protected boolean hasMoreResultSet() {
        return count < this.stmtSize;
    }


    private class ObserverWrite implements Observer<Object[]> {
        private final RowObservable rowIterable;
        private PromiseInternal<Void> promise;
        boolean moreResultSet;
        Function<Object[], byte[]> convertor;
        Disposable disposable;
        boolean end = false;

        public ObserverWrite(RowObservable rowIterable, boolean hasMoreResultSet, PromiseInternal<Void> promise) {
            this.rowIterable = rowIterable;
            this.promise = promise;
            this.moreResultSet = hasMoreResultSet;
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            this.disposable = d;
            MycatRowMetaData rowMetaData = rowIterable.getRowMetaData();
            int columnCount = rowMetaData.getColumnCount();
            session.writeColumnCount(columnCount);
            if (!binary) {
                this.convertor = ResultSetMapping.concertToDirectTextResultSet(rowMetaData);
            } else {
                this.convertor = ResultSetMapping.concertToDirectBinaryResultSet(rowMetaData);
            }
            Iterator<byte[]> columnIterator = MySQLPacketUtil.generateAllColumnDefPayload(rowMetaData).iterator();
            while (columnIterator.hasNext()) {
                session.writeBytes(columnIterator.next(), false);
            }
            session.writeColumnEndPacket();
        }

        @Override
        public void onNext(Object @NonNull [] objects) {
            session.writeBytes(this.convertor.apply(objects), false);
        }

        @Override
        public void onError(@NonNull Throwable e) {
            if (disposable != null) {
                disposable.dispose();
            }
            session.getDataContext().getTransactionSession()
                    .closeStatenmentState().onComplete(event -> promise.tryFail(e));
            return;
        }

        @Override
        public void onComplete() {
            if (disposable != null) {
                disposable.dispose();
                disposable = null;
            }
            if (!end){
                end = true;
                session.getDataContext().getTransactionSession()
                        .closeStatenmentState()
                        .onComplete(event -> session.writeRowEndPacket(moreResultSet, false)
                                .onComplete(event1 -> promise.tryComplete()));
            }else {
                LOGGER.debug("bug");
            }

        }
    }
}