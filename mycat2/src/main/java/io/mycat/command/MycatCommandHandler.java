/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.command;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.command.CommandDispatcher.AbstractCommandHandler;
import io.mycat.command.prepareStatement.PrepareStmtProxy;
import io.mycat.config.schema.SchemaType;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.PrepareStmtTask;
import io.mycat.proxy.handler.backend.RequestHandler;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.FrontSessionManager;
import io.mycat.router.MycatRouter;
import io.mycat.sqlparser.util.BufferSQLContext;
import java.util.Map;

/**
 * @author jamie12221 date 2019-05-13 02:47
 **/
public class MycatCommandHandler extends AbstractCommandHandler implements QueryHandler {

  final MycatRouter router;

  public MycatCommandHandler(MycatRouter router) {
    this.router = router;
  }

  final PrepareStmtProxy psp = new PrepareStmtProxy();

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
  public void handlePrepareStatement(byte[] bytes, MycatSession mycat) {
    MycatSchema schema = mycat.getSchema();
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }
    String sql = new String(bytes);

    BufferSQLContext bufferSQLContext = router().simpleParse(sql);
    boolean runOnMaster = bufferSQLContext.isSimpleSelect();

    if (schema.getSchema() == SchemaType.DB_IN_ONE_SERVER) {
      String dataNode = schema.getDefaultDataNode();
      MySQLDataNode node = ProxyRuntime.INSTANCE.getDataNodeByName(dataNode);
      if (!psp.existPrepareResponse(sql)) {
        MySQLTaskUtil.withBackend(mycat, runOnMaster, node, null,
            new SessionCallBack<MySQLClientSession>() {
              @Override
              public void onSession(MySQLClientSession session, Object sender, Object attr) {

                PrepareStmtTask prepareStmtHanlder = new PrepareStmtTask(mycat, psp);
                prepareStmtHanlder.requestPrepareStatement(session, sql,
                    new ResultSetCallBack<MySQLClientSession>() {
                      @Override
                      public void onFinishedSendException(Exception exception, Object sender,
                          Object attr) {
                        String message = exception.toString();
                        mycat.setLastMessage(message);
                        mycat.writeErrorEndPacketBySyncInProcessError();
                        mycat.close(false, message);
                      }

                      @Override
                      public void onFinishedException(Exception exception, Object sender,
                          Object attr) {
                        String message = exception.toString();
                        mycat.setLastMessage(message);
                        mycat.writeErrorEndPacketBySyncInProcessError();
                        mycat.close(false, message);
                      }

                      @Override
                      public void onFinished(boolean monopolize, MySQLClientSession mysql,
                          Object sender, Object attr) {
                        psp.record(mysql.getDatasource(), sql, prepareStmtHanlder.getStatementId(),
                            mysql.sessionId());
                        if (!monopolize) {
                          mycat.getMycatReactorThread().getMySQLSessionManager()
                              .addIdleSession(mysql);
                        }
                      }

                      @Override
                      public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                          MySQLClientSession mysql, Object sender, Object attr) {
                        mycat.writeErrorEndPacket(errorPacket);
                      }
                    });
              }

              @Override
              public void onException(Exception exception, Object sender, Object attr) {
                String message = exception.toString();
                mycat.setLastMessage(message);
                mycat.writeErrorEndPacketBySyncInProcessError();
                mycat.close(false, message);
              }
            });
      } else {
        mycat.setLastMessage(
            "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
        mycat.writeErrorEndPacket();
      }

      return;
    } else {
      mycat.setLastMessage(
          "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
      mycat.writeErrorEndPacket();
    }

  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession mycat) {
    MycatSchema schema = mycat.getSchema();
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }

    if (schema.getSchema() == SchemaType.DB_IN_ONE_SERVER) {
      psp.appendLongData(statementId, paramId, data);
    } else {
      mycat.setLastMessage(
          "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
      mycat.writeErrorEndPacket();
    }
  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams,
      byte[] nullMap, boolean newParamsBound, byte[] typeList, byte[] fieldList,
      MycatSession mycat) {
    if (mycat.isBindMySQLSession()) {
      prepareStatementExecute(rawPayload, statementId, mycat, mycat.getMySQLSession());
    }else {
      String defaultDataNode = mycat.getSchema().getDefaultDataNode();
      MySQLDataNode dataNode = ProxyRuntime.INSTANCE.getDataNodeByName(defaultDataNode);
      MySQLTaskUtil.withBackend(mycat, false, dataNode, null,
          new SessionCallBack<MySQLClientSession>() {
            @Override
            public void onSession(MySQLClientSession session, Object sender, Object attr) {
              prepareStatementExecute(rawPayload, statementId, mycat,session);
            }

            @Override
            public void onException(Exception exception, Object sender, Object attr) {
              exception.printStackTrace();
              String message = exception.getMessage();
              mycat.setLastMessage(message);
              mycat.writeErrorEndPacketBySyncInProcessError();
            }
          });

    }
  }

  private void prepareStatementExecute(byte[] rawPayload, long statementId, MycatSession mycat,
      MySQLClientSession mySQLSession) {
    mycat.setMySQLSession(mySQLSession);
    if (psp.existLongDataPacket(statementId)) {
      RequestHandler.INSTANCE.request(mySQLSession,
          psp.generateAllLongDataPacket(statementId), new RequestCallback() {
            @Override
            public void onFinishedSend(MySQLClientSession session, Object sender, Object attr) {
              MySQLTaskUtil
                  .proxyBackend(mycat, rawPayload, mycat.getDataNode(), false, null, false);
            }

            @Override
            public void onFinishedSendException(Exception e, Object sender, Object attr) {
              String message = e.toString();
              mycat.setMySQLSession(null);
              mycat.setLastMessage(message);
              mycat.writeErrorEndPacketBySyncInProcessError();
              mycat.close(false, message);
            }
          });
    } else {
      MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil.generateMySQLPacket(0, rawPayload),
          mycat.getMySQLSession().getDataNode().getName(), false, null, false
      );
    }
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
