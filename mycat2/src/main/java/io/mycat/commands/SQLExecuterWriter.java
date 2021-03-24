///**
// * Copyright (C) <2021>  <chen junwen>
// * <p>
// * This program is open software: you can redistribute it and/or modify it under the terms of the
// * GNU General Public License as published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// * <p>
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// * General Public License for more details.
// * <p>
// * You should have received a copy of the GNU General Public License along with this program.  If
// * not, see <http://www.gnu.org/licenses/>.
// */
//package io.mycat.commands;
//
//import io.mycat.MySQLPacketUtil;
//import io.mycat.MycatDataContext;
//import io.mycat.Response;
//import io.mycat.api.collector.RowBaseIterator;
//import io.mycat.api.collector.RowIterable;
//import io.mycat.api.collector.RowObservable;
//import io.mycat.beans.mycat.JdbcRowBaseIterator;
//import io.mycat.beans.mycat.MycatRowMetaData;
//import io.mycat.beans.mycat.TransactionType;
//import io.mycat.beans.resultset.MycatProxyResponse;
//import io.mycat.beans.resultset.MycatResponse;
//import io.mycat.beans.resultset.MycatResultSetResponse;
//import io.mycat.proxy.session.MycatSession;
//import io.mycat.resultset.BinaryResultSetResponse;
//import io.mycat.resultset.DirectTextResultSetResponse;
//import io.mycat.resultset.TextResultSetResponse;
//import io.mycat.runtime.ProxyTransactionSession;
//import io.mycat.util.VertxUtil;
//import io.mycat.vertx.ResultSetMapping;
//import io.reactivex.rxjava3.annotations.NonNull;
//import io.reactivex.rxjava3.core.Observer;
//import io.reactivex.rxjava3.disposables.Disposable;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.Future;
//import io.vertx.core.Handler;
//import io.vertx.core.impl.future.PromiseInternal;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Iterator;
//import java.util.function.Function;
//
//public class SQLExecuterWriter implements SQLExecuterWriterHandler {
//    final int total;
//    final MycatSession session;
//    final Response receiver;
//    final boolean binary;
//    int count;
//    final static Logger LOGGER = LoggerFactory.getLogger(SQLExecuterWriter.class);
//
//    public SQLExecuterWriter(int total,
//                             boolean binary,
//                             MycatSession session, Response receiver) {
//        this.total = total;
//        this.count = total;
//        this.binary = binary;
//        this.session = session;
//        this.receiver = receiver;
//
//        if (this.count == 0) {
//            throw new AssertionError();
//        }
//        if (binary) {
//            if (this.count != 1) {
//                throw new AssertionError();
//            }
//        }
//    }
//
//    @Override
//    public PromiseInternal<Void> writeToMycatSession(MycatResponse response) {
//        ProxyTransactionSession transactionSession = (ProxyTransactionSession) session.getDataContext().getTransactionSession();
//        boolean moreResultSet = !(this.count == 1);
//        try (MycatResponse mycatResponse = response) {
//            switch (mycatResponse.getType()) {
//                case RRESULTSET: {
//                    RowIterable rowIterable = (RowIterable) mycatResponse;
//                    return sendResultSet(moreResultSet, rowIterable.get());
//                }
//                case UPDATEOK: {
//                    PromiseInternal<Void> promise = VertxUtil.newPromise();
//                    transactionSession.closeStatenmentState().onComplete(event -> {
//                        promise.tryComplete();
//                        session.writeOk(moreResultSet);
//                    });
//                    return promise;
//                }
//                case ERROR: {
//
//                }
//                case PROXY: {
//                    MycatProxyResponse proxyResponse = (MycatProxyResponse) mycatResponse;
//                    switch (proxyResponse.getExecuteType()) {
//                        case QUERY:
//                        case QUERY_MASTER:
//                            transactionSession.
//                                    RowBaseIterator rowBaseIterator = connection.executeQuery(null, proxyResponse.getSql());
//                            return sendResultSet(moreResultSet, rowBaseIterator);
//                        case INSERT: {
//                            long[] res = connection.executeUpdate(proxyResponse.getSql(), true);
//                            session.setAffectedRows(res[0]);
//                            session.setLastInsertId(res[1]);
//                            transactionSession.closeStatenmentState();
//                            return session.writeOk(moreResultSet);
//                        }
//                        case UPDATE: {
//                            long[] res = connection.executeUpdate(proxyResponse.getSql(), false);
//                            session.setAffectedRows(res[0]);
//                            session.setLastInsertId(res[1]);
//                            transactionSession.closeStatenmentState();
//                            return session.writeOk(moreResultSet);
//                        }
//                        default:
//                            throw new IllegalStateException("Unexpected value: " + proxyResponse.getExecuteType());
//                    }
//                }
//                case COMMIT: {
//                    PromiseInternal<Void> promise = VertxUtil.newPromise();
//                    Future<Void> commit = transactionSession.commit();
//                    commit.onComplete(event -> transactionSession.closeStatenmentState().onComplete(unused -> {
//                        if (!event.succeeded()){
//                            promise.tryFail(event.cause());
//                        }else {
//                            promise.tryComplete();
//                            session.writeOk(moreResultSet);
//                        }
//                    }));
//                    return promise;
//                }
//                case ROLLBACK: {
//                    PromiseInternal<Void> promise = VertxUtil.newPromise();
//                    Future<Void> commit = transactionSession.rollback();
//                    commit.onComplete(event -> transactionSession.closeStatenmentState().onComplete(unused -> {
//                        if (!event.succeeded()){
//                            promise.tryFail(event.cause());
//                            sendError(event.cause());
//                        }else {
//                            promise.tryComplete();
//                            session.writeOk(moreResultSet);
//                        }
//                    }));
//                    return promise;
//                }
//                case BEGIN: {
//                    PromiseInternal<Void> promise = VertxUtil.newPromise();
//                    Future<Void> commit = transactionSession.begin();
//                    commit.onComplete(event -> transactionSession.closeStatenmentState().onComplete(unused -> {
//                        if (!event.succeeded()){
//                            promise.tryFail(event.cause());
//                        }else {
//                            promise.tryComplete();
//                            session.writeOk(moreResultSet);
//                        }
//                    }));
//                    return promise;
//                }
//                case OBSERVER_RRESULTSET: {
//                    PromiseInternal<Void> voidPromiseInternal = VertxUtil.newSuccessPromise();
//                    RowObservable rowObservable = (RowObservable) response;
//                    ObserverWrite observerWrite = new ObserverWrite(rowObservable) {
//                        @Override
//                        public void onError(@NonNull Throwable e) {
//                            super.onError(e);
//                            voidPromiseInternal.tryFail(e);
//                        }
//
//                        @Override
//                        public void onComplete() {
//                            super.onComplete();
//                            voidPromiseInternal.tryComplete();
//                        }
//                    };
//                    rowObservable.subscribe(observerWrite);
//                    return voidPromiseInternal;
//                }
//                default:
//                    throw new IllegalStateException("Unexpected value: " + mycatResponse.getType());
//            }
//        } catch (Exception e) {
//            session.setLastMessage(e);
//            return session.writeErrorEndPacketBySyncInProcessError();
//        } finally {
//            this.count--;
//        }
//
//    }
//
//    private class ObserverWrite implements Observer<Object[]> {
//        private final RowObservable rowIterable;
//        boolean moreResultSet;
//        Function<Object[], byte[]> convertor;
//        Disposable disposable;
//
//        public ObserverWrite(RowObservable rowIterable) {
//            this.rowIterable = rowIterable;
//            this.moreResultSet = count != 1;
//        }
//
//        @Override
//        public void onSubscribe(@NonNull Disposable d) {
//            this.disposable = d;
//            MycatRowMetaData rowMetaData = rowIterable.getRowMetaData();
//
//            session.writeColumnCount(rowMetaData.getColumnCount());
//            if (!binary) {
//                this.convertor = ResultSetMapping.concertToDirectTextResultSet(rowMetaData);
//            } else {
//                this.convertor = ResultSetMapping.concertToDirectBinaryResultSet(rowMetaData);
//            }
//            Iterator<byte[]> columnIterator = MySQLPacketUtil.generateAllColumnDefPayload(rowMetaData).iterator();
//            while (columnIterator.hasNext()) {
//                session.writeBytes(columnIterator.next(), false);
//            }
//            session.writeColumnEndPacket();
//        }
//
//        @Override
//        public void onNext(Object @NonNull [] objects) {
//            session.writeBytes(this.convertor.apply(objects), false);
//            ;
//        }
//
//        @Override
//        public void onError(@NonNull Throwable e) {
//            session.getDataContext().getTransactionSession().closeStatenmentState();
//            if (disposable != null) {
//                disposable.dispose();
//            }
//
//            session.setLastMessage(e);
//            session.writeErrorEndPacketBySyncInProcessError();
//            return;
//        }
//
//        @Override
//        public void onComplete() {
//            session.getDataContext().getTransactionSession().closeStatenmentState();
//            if (disposable != null) {
//                disposable.dispose();
//            }
//            session.writeRowEndPacket(moreResultSet, false);
//        }
//    }
//
//    private PromiseInternal<Void> sendResultSet(boolean moreResultSet, RowBaseIterator resultSet) {
//        PromiseInternal<Void> promise = VertxUtil.newPromise();
//        Future<Void> future = Future.future(event -> {
//            try {
//                MycatResultSetResponse currentResultSet;
//                if (!binary) {
//                    if (resultSet instanceof JdbcRowBaseIterator) {
//                        currentResultSet = new DirectTextResultSetResponse((resultSet));
//                    } else {
//                        currentResultSet = new TextResultSetResponse(resultSet);
//                    }
//                } else {
//                    currentResultSet = new BinaryResultSetResponse(resultSet);
//                }
//                session.writeColumnCount(currentResultSet.columnCount());
//                Iterator<byte[]> columnDefPayloadsIterator = currentResultSet
//                        .columnDefIterator();
//                while (columnDefPayloadsIterator.hasNext()) {
//                    session.writeBytes(columnDefPayloadsIterator.next(), false);
//                }
//                session.writeColumnEndPacket();
//                Iterator<byte[]> rowIterator = currentResultSet.rowIterator();
//                while (rowIterator.hasNext()) {
//                    byte[] row = rowIterator.next();
//                    session.writeBytes(row, false);
//                }
//                event.tryComplete();
//            } catch (Throwable throwable) {
//                event.fail(throwable);
//            }
//        });
//        future.flatMap(unused -> session.getDataContext().getTransactionSession().closeStatenmentState()
//                .flatMap(unused2 -> {
//                    promise.tryComplete();
//                    return session.writeRowEndPacket(moreResultSet, false);
//                }))
//                .recover(throwable -> session.getDataContext().getTransactionSession().closeStatenmentState()
//                        .flatMap(unused3 -> {
//                            session.setLastMessage(throwable);
//                            promise.fail(throwable);
//                            return session.writeErrorEndPacketBySyncInProcessError();
//                        }));
//        return promise;
//    }
//}