package io.mycat.proxy.command;

import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-12 21:36
 **/
public interface CommandHandler extends LocalInFileRequestHandler, PrepareStatementHandler {

  void handleQuery(byte[] sql, MycatSessionView session);

  void handleSleep(MycatSessionView session);

  void handleQuit(MycatSessionView session);

  void handleInitDb(String db, MycatSessionView session);

  void handlePing(MycatSessionView session);

  void handleFieldList(String table, String filedWildcard, MycatSessionView session);

  void handleSetOption(boolean on, MycatSessionView session);

  void handleCreateDb(String schemaName, MycatSessionView session);

  void handleDropDb(String schemaName, MycatSessionView session);

  void handleRefresh(int subCommand, MycatSessionView session);

  void handleShutdown(int shutdownType, MycatSessionView session);

  void handleStatistics(MycatSessionView session);

  void handleProcessInfo(MycatSessionView session);

  void handleConnect(MycatSessionView session);

  void handleProcessKill(long connectionId, MycatSessionView session);

  void handleDebug(MycatSessionView session);

  void handleTime(MycatSessionView session);

  void handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
      MycatSessionView session);

  void handleDelayedInsert(MycatSessionView session);

  void handleResetConnection(MycatSessionView session);

  void handleDaemon(MycatSessionView session);

  abstract class AbstractCommandHandler implements CommandHandler {

    @Override
    public void handleSleep(MycatSessionView session) {
      session.writeErrorEndPacket();
    }


    @Override
    public void handleRefresh(int subCommand, MycatSessionView session) {
      session.writeErrorEndPacket();
    }

    @Override
    public void handleShutdown(int shutdownType, MycatSessionView session) {
      session.writeErrorEndPacket();
    }


    @Override
    public void handleConnect(MycatSessionView session) {
      session.writeErrorEndPacket();
    }


    @Override
    public void handleDebug(MycatSessionView session) {
      session.writeErrorEndPacket();
    }

    @Override
    public void handleTime(MycatSessionView session) {
      session.writeErrorEndPacket();
    }


    @Override
    public void handleDelayedInsert(MycatSessionView session) {
      session.writeErrorEndPacket();
    }


    @Override
    public void handleDaemon(MycatSessionView session) {
      session.writeErrorEndPacket();
    }


  }

}
