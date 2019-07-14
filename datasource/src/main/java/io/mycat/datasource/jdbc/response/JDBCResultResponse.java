package io.mycat.datasource.jdbc.response;

import io.mycat.datasource.jdbc.MycatResponse;
import io.mycat.datasource.jdbc.MycatResultSetType;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCResultResponse implements MycatResponse {

  private final Statement statement;
  private final ResultSet resultSet;

  public JDBCResultResponse(Statement statement, ResultSet rs) {
    this.statement = statement;
    this.resultSet = rs;
  }

  public void writeToMycatSession(MycatSession session) {
    try (ResultSet r = resultSet) {
      ResultSetMetaData metaData = r.getMetaData();
      int columnCount = metaData.getColumnCount();
      session.writeColumnCount(columnCount);
      for (int i = 1; i <= columnCount; i++) {
        session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(metaData, i), false);
      }
      session.writeColumnEndPacket();
      while (r.next()) {
        byte[][] byteArray = new byte[columnCount][];
        for (int i = 0; i < columnCount; i++) {
          byte[] bytes = r.getBytes(i + 1);
          byteArray[i] = bytes;
        }
        session.writeTextRowPacket(byteArray);
      }
      session.writeRowEndPacket(false, false);
    } catch (SQLException e) {
      session.setLastMessage(e);
      session.writeErrorEndPacket();
    }
  }

  @Override
  public MycatResultSetType getType() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}