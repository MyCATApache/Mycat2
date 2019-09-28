package io.mycat.proxy;

import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.SQLExecuter;
import io.mycat.proxy.session.MycatSession;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class SQLExecuterWriter {

    public static void writeToMycatSession(MycatSession session, final SQLExecuter sqlExecuters) {
        writeToMycatSession(session, new SQLExecuter[]{sqlExecuters});
    }

    public static void writeToMycatSession(MycatSession session, final SQLExecuter... sqlExecuters) {
        if (sqlExecuters.length == 0) {
            session.writeOkEndPacket();
            return;
        }
        final SQLExecuter endSqlExecuter = sqlExecuters[sqlExecuters.length - 1];
        try {
            for (SQLExecuter sqlExecuter : sqlExecuters) {
                try (MycatResponse resultSet = sqlExecuter.execute()) {
                    switch (resultSet.getType()) {
                        case RRESULTSET: {
                            MycatResultSetResponse currentResultSet = (MycatResultSetResponse) resultSet;
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
                            session.writeRowEndPacket(endSqlExecuter != sqlExecuter, false);
                            break;
                        }
                        case UPDATEOK: {
                            MycatUpdateResponse currentUpdateResponse = (MycatUpdateResponse) resultSet;
                            int updateCount = currentUpdateResponse.getUpdateCount();
                            long lastInsertId1 = currentUpdateResponse.getLastInsertId();
                            session.setWarningCount(updateCount);
                            session.setLastInsertId(lastInsertId1);
                            session.writeOk(endSqlExecuter != sqlExecuter);
                            break;
                        }
                        case ERROR:
                            break;
                        case RRESULTSET_BYTEBUFFER: {
                            MycatResultSetResponse currentResultSet = (MycatResultSetResponse) resultSet;
                            session.writeColumnCount(currentResultSet.columnCount());
                            Iterator<ByteBuffer> columnDefPayloadsIterator = currentResultSet
                                    .columnDefIterator();
                            while (columnDefPayloadsIterator.hasNext()) {
                                session.writeBytes(columnDefPayloadsIterator.next(), false);
                            }
                            session.writeColumnEndPacket();
                            Iterator<ByteBuffer> rowIterator = currentResultSet.rowIterator();
                            while (rowIterator.hasNext()) {
                                session.writeBytes(rowIterator.next(), false);
                            }
                            session.writeRowEndPacket(endSqlExecuter != sqlExecuter, false);
                            break;
                        }
                    }
                }
            }
            return;
        } catch (Exception e) {
            session.setLastMessage(e);
        }
        session.writeErrorEndPacket();
        return;
    }
}