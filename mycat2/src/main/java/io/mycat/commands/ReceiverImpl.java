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
package io.mycat.commands;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.*;
import io.mycat.api.collector.MySQLColumnDef;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.MysqlRow;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.proxy.session.MySQLServerSession;
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
import io.vertx.core.Promise;
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
    protected final XaSqlConnection transactionSession;
    private final int stmtSize;
    private final boolean binary;
    protected int count = 0;

    public ReceiverImpl(MySQLServerSession session, int stmtSize, boolean binary) {
        this.stmtSize = stmtSize;
        this.binary = binary;
        this.session = session;
        this.dataContext = this.session.getDataContext();
        this.transactionSession = (XaSqlConnection) this.dataContext.getTransactionSession();
    }


    @Override
    public Future<Void> sendError(Throwable e) {
        MycatDataContext dataContext = session.getDataContext();
        dataContext.setLastMessage(e);
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
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);

        return ioExecutor.executeBlocking(promise1 -> mysqlPacketObservable.subscribe(
                new MysqlPayloadObjectObserver(promise1, hasMoreResult, binary, session)));

    }

    @Override
    public Future<Void> rollback() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.rollback()
                .eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> begin() {
        count++;
        boolean hasMoreResultSet = hasMoreResultSet();
        return transactionSession.begin()
                .eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> commit() {
        count++;
        boolean moreResultSet = hasMoreResultSet();
        return transactionSession.commit().eventually((u) -> transactionSession.closeStatementState())
                .flatMap(u -> session.writeOk(moreResultSet));
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
                    return sendResultSet(mysqlPacketObservable);
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
        return transactionSession.closeStatementState().flatMap(u -> session.writeOk(hasMoreResultSet));
    }

    @Override
    public Future<Void> sendOk(long affectedRow) {
        dataContext.setAffectedRows(affectedRow);
        return sendOk();
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


    public static class MysqlPayloadObjectObserver implements Observer<MysqlPayloadObject> {
        private final Promise<Void> promise;
        private final boolean moreResultSet;
        private boolean binary;
        private MySQLServerSession session;
        private Disposable disposable;
        Function<Object[], byte[]> convertor;

        public MysqlPayloadObjectObserver(Promise<Void> promise,
                                          boolean moreResultSet, boolean binary, MySQLServerSession session) {
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