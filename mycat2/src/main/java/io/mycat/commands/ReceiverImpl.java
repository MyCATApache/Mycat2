package io.mycat.commands;

import io.mycat.*;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.proxy.session.MySQLServerSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.runtime.ProxyTransactionSession;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.ResultSetMapping;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import static io.mycat.ExecuteType.*;


public class ReceiverImpl implements Response {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverImpl.class);

    protected final MySQLServerSession session;
    protected final MycatDataContext dataContext;
    protected final ProxyTransactionSession transactionSession;
    private final int stmtSize;
    private final boolean binary;
    protected int count = 0;

    public ReceiverImpl(MySQLServerSession session, int stmtSize, boolean binary) {
        this.stmtSize = stmtSize;
        this.binary = binary;
        this.session = session;
        this.dataContext = this.session.getDataContext();
        this.transactionSession = (ProxyTransactionSession) this.dataContext.getTransactionSession();
    }


    @Override
    public Future<Void> sendError(Throwable e) {
        session.getDataContext().setLastMessage(e);
        return VertxUtil.newFailPromise(new RuntimeException(e));
    }

    @Override
    public Future<Void> proxySelect(String defaultTargetName, String statement) {
        return execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }


    @Override
    public Future<Void> proxyUpdate(String defaultTargetName, String sql) {
        return execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(defaultTargetName), sql, null));
    }

    @Override
    public Future<Void> proxySelectToPrototype(String statement) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return execute(ExplainDetail.create(QUERY_MASTER, Objects.requireNonNull(metadataManager.getPrototype()), statement, null));
    }


    @Override
    public Future<Void> sendError(String errorMessage, int errorCode) {
        return VertxUtil.newFailPromise(new MycatException(errorCode, errorMessage));
    }

    @Override
    public Future<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
        count++;
        boolean hasMoreResult = hasMoreResultSet();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        mysqlPacketObservable.subscribe(
                new MysqlPayloadObjectObserver(promise, hasMoreResult, binary, session));
        return promise;
    }

    @Override
    public Future<Void> rollback() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
       return transactionSession.rollback()
               .eventually((u)-> transactionSession.closeStatementState())
                .flatMap(u->session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> begin() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return  transactionSession.begin()
                .eventually((u)-> transactionSession.closeStatementState())
                .flatMap(u->session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> commit() {
        count++;
        boolean moreResultSet = hasMoreResultSet();
        return transactionSession.commit().eventually((u)->transactionSession.closeStatementState())
                .flatMap(u->session.writeOk(moreResultSet));
    }

    @Override
    public Future<Void> execute(ExplainDetail detail) {
        boolean directPacket = false;
        boolean master = dataContext.isInTransaction() || !dataContext.isAutocommit() || detail.getExecuteType().isMaster();
        String datasource = session.getDataContext().resolveDatasourceTargetName(detail.getTarget(), master);
        String sql = detail.getSql();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        boolean inTransaction = dataContext.isInTransaction();
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
    public Future<Void> sendOk() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.closeStatementState().flatMap(u->session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> sendOk(long affectedRow, long lastInsertId) {
        dataContext.setLastInsertId(lastInsertId);
        dataContext.setAffectedRows(affectedRow);
        return sendOk();
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        return null;
    }


    protected boolean hasMoreResultSet() {
        return count < this.stmtSize;
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