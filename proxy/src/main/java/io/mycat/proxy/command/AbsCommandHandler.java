package io.mycat.proxy.command;

import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.command.CommandHandler.AbstractCommandHandler;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-13 02:47
 **/
public class AbsCommandHandler extends AbstractCommandHandler {

  @Override
  public void handleQuery(byte[] sql, MycatSessionView session) {
    String dataNode = MycatRuntime.INSTANCE
                          .getDefaultSchema().getDefaultDataNode();
    session
        .proxyBackend(dataNode, false, null, false, (session1, sender, success, result, attr) -> {
          if (success) {
            System.out.println("success full");
          } else {
            System.out.println("success fail");
          }
        });
  }

  @Override
  public void handleContentOfFilename(byte[] sql, MycatSessionView seesion) {

  }

  @Override
  public void handleContentOfFilenameEmptyOk() {

  }

  @Override
  public void handleQuit(MycatSessionView session) {

  }

  @Override
  public void handleInitDb(String db, MycatSessionView session) {

  }

  @Override
  public void handlePing(MycatSessionView session) {

  }

  @Override
  public void handleFieldList(String table, String filedWildcard, MycatSessionView session) {

  }

  @Override
  public void handleSetOption(boolean on, MycatSessionView session) {

  }

  @Override
  public void handleCreateDb(String schemaName, MycatSessionView session) {

  }

  @Override
  public void handleDropDb(String schemaName, MycatSessionView session) {

  }

  @Override
  public void handleStatistics(MycatSessionView session) {

  }

  @Override
  public void handleProcessInfo(MycatSessionView session) {

  }

  @Override
  public void handleProcessKill(long connectionId, MycatSessionView session) {

  }

  @Override
  public void handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
      MycatSessionView session) {

  }

  @Override
  public void handleResetConnection(MycatSessionView session) {

  }

  @Override
  public void handlePrepareStatement(byte[] sql, MycatSessionView session) {

  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, long paramId, byte[] data,
      MycatSessionView session) {

  }

  @Override
  public void handlePrepareStatementExecute(long statementId, byte flags, int numParams,
      byte[] nullMap,
      boolean newParamsBound, byte[] typeList, byte[] fieldList, MycatSessionView session) {

  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSessionView session) {

  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSessionView session) {

  }
}
