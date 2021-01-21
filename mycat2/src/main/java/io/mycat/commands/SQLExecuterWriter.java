/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is open software: you can redistribute it and/or modify it under the terms of the
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

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.resultset.MycatProxyResponse;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.proxy.NativeMycatServer;
import io.mycat.proxy.session.MycatSession;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.DirectTextResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.vertx.ResultSetMapping;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Function;

public class SQLExecuterWriter implements SQLExecuterWriterHandler {
    final int total;
    final MycatSession session;
    final Response receiver;
    final boolean binary;
    int count;
    final static Logger LOGGER = LoggerFactory.getLogger(SQLExecuterWriter.class);

    public SQLExecuterWriter(int total,
                             boolean binary,
                             MycatSession session, Response receiver) {
        this.total = total;
        this.count = total;
        this.binary = binary;
        this.session = session;
        this.receiver = receiver;

        if (this.count == 0) {
            throw new AssertionError();
        }
        if (binary) {
            if (this.count != 1) {
                throw new AssertionError();
            }
        }
    }

    public void writeToMycatSession(MycatResponse response) {
        TransactionSession transactionSession = session.getDataContext().getTransactionSession();
        boolean moreResultSet = !(this.count == 1);
        try{
            try (MycatResponse mycatResponse = response) {
                switch (mycatResponse.getType()) {
                    case RRESULTSET: {
                        RowIterable rowIterable = (RowIterable) mycatResponse;
                        sendResultSet(moreResultSet, rowIterable.get());
                        return;
                    }
                    case UPDATEOK: {
                        transactionSession.closeStatenmentState();
                        session.writeOk(moreResultSet);
                        return;
                    }
                    case ERROR: {
                        transactionSession.closeStatenmentState();
                        session.writeErrorEndPacketBySyncInProcessError();
                        return;
                    }
                    case PROXY: {
                        MycatProxyResponse proxyResponse = (MycatProxyResponse) mycatResponse;

                        if (this.count == 1 && transactionSession.transactionType() == TransactionType.PROXY_TRANSACTION_TYPE) {
                            NativeMycatServer mycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
                            if (mycatServer.getDatasource(proxyResponse.getTargetName()) != null) {
                                transactionSession.closeStatenmentState();
                                MySQLTaskUtil.proxyBackendByDatasourceName(session, proxyResponse.getTargetName(), proxyResponse.getSql(),
                                        MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                                        session.getIsolation());
                                return;
                            }
                        }

                        MycatConnection connection = transactionSession.getConnection(proxyResponse.getTargetName());
                        switch (proxyResponse.getExecuteType()) {
                            case QUERY:
                            case QUERY_MASTER:
                                RowBaseIterator rowBaseIterator = connection.executeQuery(null, proxyResponse.getSql());
                                sendResultSet(moreResultSet, rowBaseIterator);
                                return;
                            case INSERT: {
                                long[] res = connection.executeUpdate(proxyResponse.getSql(), true);
                                session.setAffectedRows(res[0]);
                                session.setLastInsertId(res[1]);
                                transactionSession.closeStatenmentState();
                                session.writeOk(moreResultSet);
                                return;
                            }
                            case UPDATE: {
                                long[] res = connection.executeUpdate(proxyResponse.getSql(), false);
                                session.setAffectedRows(res[0]);
                                session.setLastInsertId(res[1]);
                                transactionSession.closeStatenmentState();
                                session.writeOk(moreResultSet);
                                return;
                            }
                            default:
                                throw new IllegalStateException("Unexpected value: " + proxyResponse.getExecuteType());
                        }
                    }
                    case COMMIT: {
                        MycatDataContext dataContext = session.getDataContext();
                        TransactionType transactionType = dataContext.transactionType();
                        switch (transactionType) {
                            case PROXY_TRANSACTION_TYPE:
                                if (moreResultSet) {
                                    session.setLastMessage("unsupported mixed rollback");
                                    session.writeErrorEndPacketBySyncInProcessError();
                                    return;
                                }
                                transactionSession.commit();
                                transactionSession.closeStatenmentState();
                                if (!session.isBindMySQLSession()) {
                                    LOGGER.debug("session id:{} action: commit from unbinding session", session.sessionId());
                                    session.writeOk(false);
                                    return;
                                } else {
                                    receiver.proxyUpdate(session.getMySQLSession().getDatasourceName(), "COMMIT");
                                    LOGGER.debug("session id:{} action: commit from binding session", session.sessionId());
                                    return;
                                }
                            case JDBC_TRANSACTION_TYPE: {
                                transactionSession.commit();
                                transactionSession.closeStatenmentState();
                                LOGGER.debug("session id:{} action: commit from xa", session.sessionId());
                                session.writeOk(moreResultSet);
                                return;
                            }
                            default:
                                throw new IllegalStateException("Unexpected value: " + transactionType);
                        }
                        //break;
                    }
                    case ROLLBACK: {
                        MycatDataContext dataContext = session.getDataContext();
                        TransactionType transactionType = dataContext.transactionType();
                        switch (transactionType) {
                            case PROXY_TRANSACTION_TYPE:
                                if (moreResultSet) {
                                    session.setLastMessage("unsupported mixed rollback");
                                    session.writeErrorEndPacketBySyncInProcessError();
                                    return;
                                }
                                transactionSession.rollback();
                                transactionSession.closeStatenmentState();
                                if (session.isBindMySQLSession()) {
                                    receiver.proxyUpdate(session.getMySQLSession().getDatasourceName(), "ROLLBACK");
                                    LOGGER.debug("session id:{} action: rollback from binding session", session.sessionId());
                                    return;
                                } else {
                                    session.writeOk(false);
                                    LOGGER.debug("session id:{} action: rollback from unbinding session", session.sessionId());
                                    return;
                                }
                            case JDBC_TRANSACTION_TYPE: {
                                transactionSession.rollback();
                                transactionSession.closeStatenmentState();
                                LOGGER.debug("session id:{} action: rollback from xa", session.sessionId());
                                session.writeOk(moreResultSet);
                                return;
                            }
                            default:
                                throw new IllegalStateException("Unexpected value: " + transactionType);
                        }
                    }
                    case BEGIN: {
                        MycatDataContext dataContext = session.getDataContext();
                        TransactionType transactionType = dataContext.transactionType();
                        switch (transactionType) {
                            case PROXY_TRANSACTION_TYPE: {
                                transactionSession.begin();
                                transactionSession.closeStatenmentState();
                                LOGGER.debug("session id:{} action:{}", session.sessionId(), "begin exe success");
                                session.writeOk(moreResultSet);
                                return;
                            }
                            case JDBC_TRANSACTION_TYPE: {
                                transactionSession.begin();
                                transactionSession.closeStatenmentState();
                                LOGGER.debug("session id:{} action: begin from xa", session.sessionId());
                                session.writeOk(moreResultSet);
                                return;
                            }
                            default:
                                throw new IllegalStateException("Unexpected value: " + transactionType);
                        }
                    }
                    case OBSERVER_RRESULTSET: {
                        RowObservable rowObservable = (RowObservable) response;
                        ObserverWrite observerWrite = new ObserverWrite(rowObservable);
                        rowObservable.subscribe(observerWrite);
                        break;
                    }
                    default:
                        throw new IllegalStateException("Unexpected value: " + mycatResponse.getType());
                }
            } catch (Exception e) {
                session.setLastMessage(e);
                session.writeErrorEndPacketBySyncInProcessError();
            } finally {
                this.count--;
            }
        }finally {

        }

    }

    private class ObserverWrite implements Observer<Object[]> {
        private final RowObservable rowIterable;
        boolean moreResultSet;
        Function<Object[], byte[]> convertor;
        Disposable disposable;

        public ObserverWrite(RowObservable rowIterable) {
            this.rowIterable = rowIterable;
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            this.disposable = d;
            MycatRowMetaData rowMetaData = rowIterable.getRowMetaData();
            this. moreResultSet = count !=0;
            session.writeColumnCount(rowMetaData.getColumnCount());
            if(!binary){
                this.convertor = ResultSetMapping.concertToDirectTextResultSet(rowMetaData);
            }else {
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
            session.writeBytes(this.convertor.apply(objects), false);;
        }

        @Override
        public void onError(@NonNull Throwable e) {
            session.getDataContext().getTransactionSession().closeStatenmentState();
            disposable.dispose();
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
            return;
        }

        @Override
        public void onComplete() {
            session.getDataContext().getTransactionSession().closeStatenmentState();
            disposable.dispose();
            session.writeRowEndPacket(moreResultSet, false);
        }
    }

    private void sendResultSet(boolean moreResultSet, RowBaseIterator resultSet) {
        MycatResultSetResponse currentResultSet;
        if (!binary) {
            if (resultSet instanceof JdbcRowBaseIterator){
                currentResultSet = new DirectTextResultSetResponse((resultSet));
            }else {
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
        currentResultSet.close();
        session.getDataContext().getTransactionSession().closeStatenmentState();
        session.writeRowEndPacket(moreResultSet, false);
    }
}