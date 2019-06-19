package io.mycat.command.prepareStatement;

import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.beans.mysql.packet.PacketSplitterImpl;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.MySQLSessionSyncTask;
import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.handler.backend.PrepareStmtTask;
import io.mycat.proxy.handler.backend.RequestHandler;
import io.mycat.proxy.handler.backend.SessionSyncCallback;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.CheckResult;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import io.mycat.replica.MySQLDatasource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class PrepareInfo {

  private long mycatStmtId;
  private String sql;
  private String dataNodeName;
  private MySQLDatasource datasource;
  private ArrayList<PrepareMySQLSessionInfo> sessionInfos = new ArrayList<>();
  private MycatSession mycat;
  private MySQLSessionManager manager;
  private HashMap<Integer, MySQLPayloadWriter> parmaMap = new HashMap<>();
  private int numOfParams = Integer.MIN_VALUE;

  public PrepareInfo(long mycatStmtId, String sql, String dataNodeName, MySQLDatasource datasource,
      MycatSession mycat, MySQLSessionManager manager) {
    this.mycatStmtId = mycatStmtId;
    this.sql = sql;
    this.dataNodeName = dataNodeName;
    this.datasource = datasource;
    this.mycat = mycat;
    this.manager = manager;
  }

  public void getPrepareSession(PrepareSessionCallback callBack, boolean proxyPrepareResponse) {
    List<PrepareMySQLSessionInfo> prepareMySQLSessionInfos = checkVaildAndGetIdleMySQLSessionIds();
    List<SessionIdAble> ids = (List) prepareMySQLSessionInfos;
    manager.getIdleSessionsOfIds(datasource, ids,
        new SessionCallBack<MySQLClientSession>() {
          @Override
          public void onSession(MySQLClientSession session, Object sender, Object attr) {
            MySQLSynContext mySQLSyn = new MySQLSynContext(mycat);
            MySQLSessionSyncTask syncTask = new MySQLSessionSyncTask(mySQLSyn, session,
                this, new SessionSyncCallback() {
              @Override
              public void onSession(MySQLClientSession session, Object sender, Object attr) {
                int id = session.sessionId();
                long statementId = Long.MIN_VALUE;
                boolean found = false;
                int size = prepareMySQLSessionInfos.size();
                for (int i = 0; i < size; i++) {
                  PrepareMySQLSessionInfo info = prepareMySQLSessionInfos.get(i);
                  if (id == info.getSessionId()) {
                    statementId = info.getStatementId();
                    found = true;
                  }
                }
                if (found) {
                  callBack.onPrepare(statementId, session);
                  return;
                } else {
                  final long stmtId = statementId;
                  PrepareStmtTask prepareStmtTask = new PrepareStmtTask(mycat, mycatStmtId, sql,
                      proxyPrepareResponse);
                  prepareStmtTask.requestPrepareStatement(session,
                      new ResultSetCallBack<MySQLClientSession>() {
                        @Override
                        public void onFinishedSendException(Exception exception, Object sender,
                            Object attr) {
                          callBack.onException(exception, sender, attr);
                        }

                        @Override
                        public void onFinishedException(Exception exception, Object sender,
                            Object attr) {
                          callBack.onException(exception, sender, attr);
                        }

                        @Override
                        public void onFinished(boolean monopolize, MySQLClientSession mysql,
                            Object sender, Object attr) {
                          long statementId = prepareStmtTask.getMysqlStatementId();
                          sessionInfos
                              .add(new PrepareMySQLSessionInfo(statementId, mysql.sessionId()));
                          if (numOfParams < 0) {
                            numOfParams = prepareStmtTask.getNumOdParmas();
                          } else {
                            assert numOfParams == prepareStmtTask.getNumOdParmas();
                          }
                          if (monopolize){
                            mycat.setMySQLSession(mysql);
                          }else {
                            mysql.setCallBack(null);
                            mysql.setMycatSession(null);
                            mysql.switchNioHandler(null);
                            manager.addIdleSession(mysql);
                          }
                        }

                        @Override
                        public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                            MySQLClientSession mysql, Object sender, Object attr) {
                          callBack.onErrorPacket(errorPacket, monopolize, mysql, this, attr);
                        }
                      });
                }
              }

              @Override
              public void onException(Exception exception, Object sender, Object attr) {
                callBack.onException(exception, sender, attr);
              }

              @Override
              public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                  MySQLClientSession mysql, Object sender, Object attr) {
                callBack.onErrorPacket(errorPacket, monopolize, mysql, sender, attr);
              }
            });
            syncTask.run();
          }

          @Override
          public void onException(Exception exception, Object sender, Object attr) {
            callBack.onException(exception, sender, attr);
          }
        });
  }

  private List<PrepareMySQLSessionInfo> checkVaildAndGetIdleMySQLSessionIds() {
    ArrayList<PrepareMySQLSessionInfo> remove = null;
    ArrayList<PrepareMySQLSessionInfo> res = new ArrayList<>();
    if (sessionInfos.isEmpty()){
      return Collections.emptyList();
    }
    for (PrepareMySQLSessionInfo sessionInfo : sessionInfos) {
      CheckResult checkResult = this.manager.check(sessionInfo.getSessionId());
      switch (checkResult) {
        case NOT_EXIST: {
          if (remove == null) {
            remove = new ArrayList<>();
          }
          remove.add(sessionInfo);
          break;
        }
        case IDLE:
          res.add(sessionInfo);
          break;
        case BUSY:
          break;
      }
    }
    if (remove != null) {
      sessionInfos.removeAll(remove);
    }
    return res;
  }

  public void appendLongData(int paramId, byte[] data) {
    parmaMap.compute(paramId,
        (integer, writer) -> {
          if (writer == null) {
            writer = new MySQLPayloadWriter();
          }
          writer.writeBytes(data);
          return writer;
        });
  }

  public void reset() {
    for (Entry<Integer, MySQLPayloadWriter> entry : parmaMap.entrySet()) {
      MySQLPayloadWriter remove = entry.getValue();
      if (remove != null) {
        remove.close();
      }
    }
    if (mycat.isBindMySQLSession()) {
      MySQLClientSession session = mycat.getMySQLSession();
      long statementId = session.getCursorStatementId();
      MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil.generateResetPacket(statementId),
          session.getDataNode().getName(), null);
      return;
    }
  }

  public void close() {
    reset();
    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
    MySQLSessionManager manager = thread.getMySQLSessionManager();
    for (PrepareMySQLSessionInfo sessionInfo : sessionInfos) {
      int sessionId = sessionInfo.getSessionId();
      long statementId = sessionInfo.getStatementId();
      switch (manager.check(sessionId)) {
        case NOT_EXIST:
          break;
        case IDLE:
        case BUSY:
          manager.appendClearRequest(sessionInfo.getSessionId(),
              MySQLPacketUtil.generateClosePacket(statementId));
          break;
      }
    }
  }

  public void execute(long statementId, byte flags, int numParams,
      byte[] rest) {
    getPrepareSession(new PrepareSessionCallback() {
      @Override
      public void onPrepare(long actualStatementId, MySQLClientSession session) {
        session.setCursorStatementId(actualStatementId);
        if (existLongData()){
          RequestHandler.INSTANCE.request(session, generateAllLongDataPacket(session),
              new RequestCallback() {
                @Override
                public void onFinishedSend(MySQLClientSession session, Object sender, Object attr) {
                  mycat.setMySQLSession(session);
                  innerExecute(actualStatementId, flags, numParams,rest);
                }

                @Override
                public void onFinishedSendException(Exception e, Object sender, Object attr) {
                  mycat.setLastMessage(e.toString());
                  mycat.writeErrorEndPacket();
                }
              });
        }else {
          innerExecute(statementId, flags, numParams, rest);
        }
        return;
      }

      @Override
      public void onException(Exception exception, Object sender, Object attr) {
        mycat.setLastMessage(exception.getMessage());
        mycat.writeErrorEndPacket();
      }

      @Override
      public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
          MySQLClientSession mysql, Object sender, Object attr) {
        mycat.setLastMessage(errorPacket.getErrorMessageString());
        mycat.writeErrorEndPacket();
      }
    }, false);
    return;
  }

  private void innerExecute(long statementId, byte flags, int numParams, byte[] rest) {
    MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil
            .generateExecutePayload(statementId, flags, numParams, rest),
        mycat.getDataNode(), null);
  }

  /**
   * Getter for property 'numOfParams'.
   *
   * @return Value for property 'numOfParams'.
   */
  public int getNumOfParams() {
    return numOfParams;
  }

  public void fetch(long numOfRows) {
    MySQLClientSession mySQLSession = mycat.getMySQLSession();
    MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil
            .generateFetchPacket(mySQLSession.getCursorStatementId(), numOfRows),
        mycat.getDataNode(), null);
  }

  public boolean existLongData() {
    return !this.parmaMap.isEmpty();
  }

  public byte[] generateAllLongDataPacket(MySQLClientSession session) {
    MySQLPacketSplitter packetSplitter = new PacketSplitterImpl();
    int packetId = 0;

    int sum = 0;
    for (Entry<Integer, MySQLPayloadWriter> entry : parmaMap.entrySet()) {
      sum += MySQLPacketSplitter.caculWholePacketSize(entry.getValue().size());
    }

    try (MySQLPayloadWriter writer = new MySQLPayloadWriter(sum)) {
      for (Entry<Integer, MySQLPayloadWriter> entry : parmaMap.entrySet()) {
        Integer key = entry.getKey();
        byte[] data = entry.getValue().toByteArray();
        packetSplitter.init(data.length);
        while (packetSplitter.nextPacketInPacketSplitter()) {
          byte[] payload = MySQLPacketUtil.generateLondData(session.getCursorStatementId(), key, data);
          byte[] packet = MySQLPacketUtil.generateMySQLPacket(packetId, payload);
          writer.writeBytes(packet);
          ++packetId;
        }
      }
      return writer.toByteArray();
    }
  }
}