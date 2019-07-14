package io.mycat.grid;

import io.mycat.proxy.packet.ColumnDefPacketImpl;
import io.mycat.proxy.session.MycatSession;

public class MycatResponseHelper {

  public static void writeToMycatSession(final SQLExecuter sqlExecuters) {
    writeToMycatSession(new SQLExecuter[]{sqlExecuters});
  }

  public static void writeToMycatSession(final SQLExecuter[] sqlExecuters) {
    final SQLExecuter endSqlExecuter = sqlExecuters[sqlExecuters.length - 1];
    MycatSession session = endSqlExecuter.getMycatSession();
    try {
      for (SQLExecuter sqlExecuter : sqlExecuters) {
        MycatResponse resultSet = sqlExecuter.execute();
        switch (resultSet.getType()) {
          case RRESULTSET:
            MycatResultSetResponse currentResultSet = (MycatResultSetResponse) resultSet;
            session.writeColumnCount(currentResultSet.getColumnCount());
            for (ColumnDefPacketImpl columnDefPacket : currentResultSet.getColumnDefPayloads()) {
              session.writeColumnDef(columnDefPacket);
            }
            session.writeColumnEndPacket();
            for (byte[][] row : currentResultSet.getRows()) {
              session.writeTextRowPacket(row);
            }
            session.writeRowEndPacket(endSqlExecuter == sqlExecuter, false);
            break;
          case UPDATEOK:
            MycatUpdateResponse currentUpdateResponse = (MycatUpdateResponse) resultSet;
            long updateCount = currentUpdateResponse.getUpdateCount();
            long lastInsertId1 = currentUpdateResponse.getLastInsertId();
            session.setWarningCount(updateCount);
            session.setLastInsertId(lastInsertId1);
            session.writeRowEndPacket(endSqlExecuter == sqlExecuter, false);
            break;
        }
      }
      return;
    } catch (Exception e) {
      session.setLastMessage(e);
    }
    session.writeErrorEndPacket();
    return;
//    try (ResultSet r = resultSet) {
//      ResultSetMetaData metaData = r.getMetaData();
//      int columnCount = metaData.getColumnCount();
//      session.writeColumnCount(columnCount);
//      for (int i = 1; i <= columnCount; i++) {
//        session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(metaData, i), false);
//      }
//      session.writeColumnEndPacket();
//      while (r.next()) {
//        byte[][] byteArray = new byte[columnCount][];
//        for (int i = 0; i < columnCount; i++) {
//          byte[] bytes = r.getBytes(i + 1);
//          byteArray[i] = bytes;
//        }
//        session.writeTextRowPacket(byteArray);
//      }
//      session.writeRowEndPacket(false, false);
//    } catch (SQLException e) {
//      session.setLastMessage(e);
//      session.writeErrorEndPacket();
//    }
  }
}