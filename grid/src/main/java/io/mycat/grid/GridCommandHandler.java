package io.mycat.grid;

import io.mycat.command.CommandDispatcher.AbstractCommandHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import java.util.Map;

public class GridCommandHandler extends AbstractCommandHandler {

  @Override
  public void initRuntime(MycatSession session, ProxyRuntime runtime) {

  }

  @Override
  public void handleQuery(byte[] sql, MycatSession session) {
    session.writeOkEndPacket();
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