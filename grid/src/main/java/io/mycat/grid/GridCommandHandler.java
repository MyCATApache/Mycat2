package io.mycat.grid;


import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.compute.RowBaseIterator;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.JdbcSession;
import io.mycat.datasource.jdbc.response.JDBCResponse;
import io.mycat.datasource.jdbc.response.JDBCResultResponse;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import java.sql.SQLException;
import java.util.Map;

public class GridCommandHandler extends AbstractCommandHandler {

  private final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(GridCommandHandler.class);
  private JdbcRuntime jdbcRuntime;

  @Override
  public void initRuntime(MycatSession session, ProxyRuntime runtime) {
    Map<String, Object> defContext = runtime.getDefContext();
    jdbcRuntime = (JdbcRuntime) defContext.get("jdbcRuntime");
  }

  @Override
  public void handleQuery(byte[] sqlBytes, MycatSession session) {
    JdbcSession jdbcSession = jdbcRuntime
        .getJdbcSessionByDataNodeName("dn1", MySQLIsolation.READ_UNCOMMITTED,
            MySQLAutoCommit.ON, null);
    String sql = new String(sqlBytes).toUpperCase();
    LOGGER.info(sql);
    RowBaseIterator iterator = null;
    try {
      if (sql.contains("SELECT")) {
        JDBCResponse jdbcResponse = jdbcSession.query(sql);
        System.out.println(jdbcResponse);
        if (jdbcResponse instanceof JDBCResultResponse) {
          ((JDBCResultResponse) jdbcResponse).writeToMycatSession(session);
        }
      } else {
        //  JDBCResponse jdbcResponse = jdbcSession.update(sql);
        //  System.out.println(jdbcResponse);
        session.writeOkEndPacket();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession session) {

  }

  @Override
  public void handleContentOfFilenameEmptyOk(MycatSession session) {

  }

  @Override
  public void handleQuit(MycatSession session) {

  }

  @Override
  public void handleInitDb(String db, MycatSession session) {

  }

  @Override
  public void handlePing(MycatSession session) {

  }

  @Override
  public void handleFieldList(String table, String filedWildcard, MycatSession session) {

  }

  @Override
  public void handleSetOption(boolean on, MycatSession session) {

  }

  @Override
  public void handleCreateDb(String schemaName, MycatSession session) {

  }

  @Override
  public void handleDropDb(String schemaName, MycatSession session) {

  }

  @Override
  public void handleStatistics(MycatSession session) {

  }

  @Override
  public void handleProcessInfo(MycatSession session) {

  }

  @Override
  public void handleProcessKill(long connectionId, MycatSession session) {

  }

  @Override
  public void handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
      MycatSession session) {

  }

  @Override
  public void handleResetConnection(MycatSession session) {

  }

  @Override
  public void handlePrepareStatement(byte[] sql, MycatSession session) {

  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession session) {

  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams, byte[] rest, MycatSession session) {

  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {

  }

  @Override
  public void handlePrepareStatementFetch(long statementId, long row,
      MycatSession mycat) {

  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession session) {

  }

  @Override
  public int getNumParamsByStatementId(long statementId, MycatSession mycat) {
    return 0;
  }
}