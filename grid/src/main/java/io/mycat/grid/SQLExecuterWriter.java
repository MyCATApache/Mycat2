package io.mycat.grid;

import io.mycat.datasource.jdbc.MycatResponse;
import io.mycat.datasource.jdbc.MycatResultSetResponse;
import io.mycat.datasource.jdbc.MycatUpdateResponse;
import io.mycat.proxy.session.MycatSession;
import java.util.Iterator;

public class SQLExecuterWriter {

  public static void writeToMycatSession(MycatSession session,final SQLExecuter sqlExecuters) {
    writeToMycatSession(session,new SQLExecuter[]{sqlExecuters});
  }

  public static void writeToMycatSession(MycatSession session,final SQLExecuter... sqlExecuters) {
    final SQLExecuter endSqlExecuter = sqlExecuters[sqlExecuters.length - 1];
    try {
      for (SQLExecuter sqlExecuter : sqlExecuters) {
        try (MycatResponse resultSet = sqlExecuter.execute()) {
            switch (resultSet.getType()) {
              case RRESULTSET:
                MycatResultSetResponse currentResultSet = (MycatResultSetResponse) resultSet;
                session.writeColumnCount(currentResultSet.columnCount());
                Iterator<byte[]> columnDefPayloadsIterator = currentResultSet
                    .columnDefIterator();
                while (columnDefPayloadsIterator.hasNext()) {
                  session.writeBytes(columnDefPayloadsIterator.next(), false);
                }
                session.writeColumnEndPacket();
                Iterator<byte[][]> rowIterator = currentResultSet.rowIterator();
                while (rowIterator.hasNext()) {
                  session.writeTextRowPacket(rowIterator.next());
                }
                session.writeRowEndPacket(endSqlExecuter != sqlExecuter, false);
                break;
              case UPDATEOK:
                MycatUpdateResponse currentUpdateResponse = (MycatUpdateResponse) resultSet;
                int updateCount = currentUpdateResponse.getUpdateCount();
                long lastInsertId1 = currentUpdateResponse.getLastInsertId();
                session.setWarningCount(updateCount);
                session.setLastInsertId(lastInsertId1);
                session.writeOk(endSqlExecuter != sqlExecuter);
                break;
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