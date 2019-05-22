package io.mycat.command;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.command.CommandDispatcher.AbstractCommandHandler;
import io.mycat.config.schema.SchemaType;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import io.mycat.router.MycatRouter;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-13 02:47
 **/
public class MycatCommandHandler extends AbstractCommandHandler implements QueryHandler {

  final MycatRouter router;

  public MycatCommandHandler(MycatRouter router) {
    this.router = router;
  }

  @Override
  public void handleQuery(byte[] sqlBytes, MycatSession mycat) {
    doQuery(sqlBytes, mycat);
  }


  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession seesion) {

  }

  @Override
  public void handleContentOfFilenameEmptyOk() {

  }

  @Override
  public void handleQuit(MycatSession mycat) {
    mycat.close(true, "quit");
  }

  @Override
  public void handleInitDb(String db, MycatSession mycat) {
    mycat.setLastMessage("mycat unsupport  handleInitDb");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handlePing(MycatSession mycat) {
    mycat.writeOkEndPacket();
  }

  @Override
  public void handleFieldList(String table, String filedWildcard, MycatSession mycat) {
    mycat.setLastMessage("mycat unsupport  handleFieldList");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleSetOption(boolean on, MycatSession mycat) {
    mycat.setMultiStatementSupport(on);
    mycat.writeOkEndPacket();
    return;
  }

  @Override
  public void handleCreateDb(String schemaName, MycatSession mycat) {
    mycat.setLastMessage("mycat unsupport handleCreateDb");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleDropDb(String schemaName, MycatSession mycat) {
    mycat.setLastMessage("mycat unsupport  handleDropDb");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleStatistics(MycatSession mycat) {
    mycat.setLastMessage("mycat unsupport  handleStatistics");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleProcessInfo(MycatSession mycat) {
    mycat.setLastMessage("mycat unsupport  handleProcessInfo");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleProcessKill(long connectionId, MycatSession mycat) {
    MycatReactorThread[] mycatReactorThreads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
    MycatReactorThread currentThread = mycat.getMycatReactorThread();
    for (MycatReactorThread mycatReactorThread : mycatReactorThreads) {
      FrontSessionManager<MycatSession> frontManager = mycatReactorThread.getFrontManager();
      for (MycatSession allSession : frontManager.getAllSessions()) {
        if (allSession.sessionId() == connectionId) {
          if (currentThread == mycatReactorThread) {
            allSession.close(true, "processKill");
          } else {
            mycatReactorThread.addNIOJob(() -> {
              allSession.close(true, "processKill");
            });
          }
          mycat.writeOkEndPacket();
          return;
        }
      }
    }
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleChangeUser(String userName, String authResponse, String schemaName,
      int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
      MycatSession mycat) {
    mycat.setLastMessage("mycat unsupport  handleChangeUser");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleResetConnection(MycatSession mycat) {
    mycat.resetSession();
    mycat.setLastMessage("mycat unsupport  handleResetConnection");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handlePrepareStatement(byte[] sql, MycatSession mycat) {
    MycatSchema schema = mycat.getSchema();
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }

    if (schema.getSchema() == SchemaType.DB_IN_ONE_SERVER) {
      String defaultDataNode = schema.getDefaultDataNode();
      MySQLTaskUtil.proxyBackend(mycat, sql, defaultDataNode, false, null, false
      );
      return;
    } else {
      mycat.setLastMessage(
          "PrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
      mycat.writeErrorEndPacket();
    }

  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, long paramId, byte[] data,
      MycatSession mycat) {
    MycatSchema schema = mycat.getSchema();
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }

    if (schema.getSchema() == SchemaType.DB_IN_ONE_SERVER) {
      String defaultDataNode = schema.getDefaultDataNode();
      byte[] bytes = MySQLPacketUtil.generateLondData(statementId, paramId, data);
      MySQLTaskUtil.proxyBackend(mycat, bytes, defaultDataNode, false, null, false
      );
      return;
    } else {
      mycat.setLastMessage(
          "PrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
      mycat.writeErrorEndPacket();
    }
  }

  @Override
  public void handlePrepareStatementExecute(long statementId, byte flags, int numParams,
      byte[] nullMap,
      boolean newParamsBound, byte[] typeList, byte[] fieldList, MycatSession session) {

  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {

  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession session) {

  }

  @Override
  public MycatRouter router() {
    return router;
  }
}
