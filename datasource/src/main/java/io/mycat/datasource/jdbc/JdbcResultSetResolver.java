//package io.mycat.grid;
//
//import io.mycat.grid.response.JDBCErrorResponse;
//import io.mycat.grid.response.JDBCResultResponse;
//import io.mycat.logTip.MycatLogger;
//import io.mycat.logTip.MycatLoggerFactory;
//import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//public class JdbcResultSetResolver {
//
//  private final static MycatLogger LOGGER = MycatLoggerFactory
//      .getLogger(JdbcResultSetResolver.class);
//
//  public static MycatResponse execute(Statement statement, String sql, boolean needGeneratedKeys) {
//    try {
//      if (needGeneratedKeys) {
//        statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
//        ResultSet generatedKeys = statement.getGeneratedKeys();
//        long lastInsertId = generatedKeys.next() ? generatedKeys.getLong(0) : 0L;
//        return new MycatUpdateResponseImpl(statement.getUpdateCount(), lastInsertId);
//      } else {
//        ResultSet resultSet = statement.executeQuery(sql);
//        ResultSetMetaData metaData = resultSet.getMetaData();
//        int columnCount = metaData.columnCount();
//        return new MycatResultSetResponse() {
//          @Override
//          public int columnCount() {
//            return columnCount;
//          }
//
//          @Override
//          public ColumnDefPacketImpl[] columnDefIterator() {
//            ColumnDefPacketImpl[] columnDefPackets = new ColumnDefPacketImpl[columnCount];
//
//            return new ColumnDefPacketImpl[0];
//          }
//
//          @Override
//          public Iterable<byte[][]> rowIterator() {
//            return null;
//          }
//        };
//      }
//    } catch (SQLException e) {
//      LOGGER.error("", e);
//
//    }
//
//  }
//}