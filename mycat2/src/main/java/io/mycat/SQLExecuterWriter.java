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
package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.resultset.MycatProxyResponse;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.proxy.session.MycatSession;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;

import java.util.Iterator;

public class SQLExecuterWriter implements SQLExecuterWriterHandler {
    final int total;
    final MycatSession session;
    final boolean binary;
    final boolean explain;
    int count;


    public SQLExecuterWriter(int total,
                             boolean binary,
                             boolean explain,
                             MycatSession session) {
        this.total = total;
        this.count = total;
        this.binary = binary;
        this.explain = explain;
        this.session = session;

        if (this.count == 0) {
            throw new AssertionError();
        }
        if (binary) {
            if (this.count != 1) {
                throw new AssertionError();
            }
        }
        if (explain && this.count != 1) {
            throw new UnsupportedOperationException();
        }
    }

    public void writeToMycatSession(MycatResponse response) {
        if (explain) {
            sendResultSet(true, response.explain());
            return;
        }
        boolean moreResultSet = !(this.count == 1);
        try (MycatResponse mycatResponse = response) {
            switch (mycatResponse.getType()) {
                case RRESULTSET: {
                    RowIterable rowIterable= (RowIterable) mycatResponse;
                    sendResultSet(moreResultSet,rowIterable.get());
                    break;
                }
                case UPDATEOK: {
                    session.writeOk(moreResultSet);
                    break;
                }
                case ERROR: {
                    session.writeErrorEndPacketBySyncInProcessError();
                    break;
                }
                case PROXY: {
                    MycatProxyResponse proxyResponse = (MycatProxyResponse) mycatResponse;
                    if (this.count == 1) {
                        if (MycatDatasourceUtil.isProxyDatasource(proxyResponse.getTargetName())) {
                            MySQLTaskUtil.proxyBackendByDatasourceName(session, proxyResponse.getTargetName(), proxyResponse.getSql(),
                                    MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                                    session.getIsolation());
                            return;
                        }
                    }
                    TransactionSession transactionSession = session.getDataContext().getTransactionSession();
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
                            session.writeOk(moreResultSet);
                            break;
                        }
                        case UPDATE: {
                            long[] res = connection.executeUpdate(proxyResponse.getSql(), false);
                            session.setAffectedRows(res[0]);
                            session.setLastInsertId(res[1]);
                            session.writeOk(moreResultSet);
                            break;
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
        } finally {
            this.count--;
        }
    }

    private void sendResultSet(boolean end, RowBaseIterator resultSet) {
        MycatResultSetResponse currentResultSet;
        if (!binary) {
            currentResultSet = new TextResultSetResponse(resultSet);
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
            session.writeBytes(rowIterator.next(), false);
        }
        session.writeRowEndPacket(end, false);
    }

    public boolean isExplain() {
        return explain;
    }
}