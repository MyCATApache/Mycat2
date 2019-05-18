package io.mycat.command;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.config.schema.SchemaType;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.MycatSessionView;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.handler.CommandHandler.AbstractCommandHandler;
import io.mycat.proxy.packet.MySQLPacketUtil;
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
  public void handleQuery(byte[] sqlBytes, MycatSessionView mycat) {
    doQuery(sqlBytes, mycat);
  }


  @Override
  public void handleContentOfFilename(byte[] sql, MycatSessionView seesion) {

  }

  @Override
  public void handleContentOfFilenameEmptyOk() {

  }

  @Override
  public void handleQuit(MycatSessionView mycat) {
    mycat.close(true, "quit");
  }

  @Override
  public void handleInitDb(String db, MycatSessionView mycat) {
    mycat.setLastMessage("mycat unsupport  handleInitDb");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handlePing(MycatSessionView mycat) {
    mycat.writeOkEndPacket();
  }

  @Override
  public void handleFieldList(String table, String filedWildcard, MycatSessionView mycat) {
    mycat.setLastMessage("mycat unsupport  handleFieldList");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleSetOption(boolean on, MycatSessionView mycat) {
    mycat.setMultiStatementSupport(on);
    mycat.writeOkEndPacket();
    return;
  }

  @Override
  public void handleCreateDb(String schemaName, MycatSessionView mycat) {
    mycat.setLastMessage("mycat unsupport handleCreateDb");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleDropDb(String schemaName, MycatSessionView mycat) {
    mycat.setLastMessage("mycat unsupport  handleDropDb");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleStatistics(MycatSessionView mycat) {
    mycat.setLastMessage("mycat unsupport  handleStatistics");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleProcessInfo(MycatSessionView mycat) {
    mycat.setLastMessage("mycat unsupport  handleProcessInfo");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleProcessKill(long connectionId, MycatSessionView mycat) {
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
      MycatSessionView mycat) {
    mycat.setLastMessage("mycat unsupport  handleChangeUser");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handleResetConnection(MycatSessionView mycat) {
    mycat.resetSession();
    mycat.setLastMessage("mycat unsupport  handleResetConnection");
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handlePrepareStatement(byte[] sql, MycatSessionView mycat) {
    MycatSchema schema = mycat.getSchema();
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }

    if (schema.getSchema() == SchemaType.DB_IN_ONE_SERVER) {
      String defaultDataNode = schema.getDefaultDataNode();
      mycat.proxyBackend(sql, defaultDataNode, false, null, false,
          (session, sender, success, result, attr) -> {
            if (!success) {
              mycat.setLastMessage(result.toString());
              mycat.writeErrorEndPacket();
            }
          });
      return;
    } else {
      mycat.setLastMessage(
          "PrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
      mycat.writeErrorEndPacket();
    }

  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, long paramId, byte[] data,
      MycatSessionView mycat) {
    MycatSchema schema = mycat.getSchema();
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }

    if (schema.getSchema() == SchemaType.DB_IN_ONE_SERVER) {
      String defaultDataNode = schema.getDefaultDataNode();
      byte[] bytes = MySQLPacketUtil.generateLondData(statementId, paramId, data);
      mycat.proxyBackend(bytes, defaultDataNode, false, null, false,
          (session, sender, success, result, attr) -> {
            if (!success) {
              mycat.setLastMessage(result.toString());
              mycat.writeErrorEndPacket();
            }
          });
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
      boolean newParamsBound, byte[] typeList, byte[] fieldList, MycatSessionView session) {

  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSessionView session) {

  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSessionView session) {

  }

  @Override
  public MycatRouter router() {
    return router;
  }
}
