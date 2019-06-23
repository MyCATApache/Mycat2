package io.mycat.command.prepareStatement;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLReplica;
import java.util.HashMap;

public class PrepareStmtContext {
  long stmtId = 0L;
  private MycatSession mycat;
  private final HashMap<Long, PrepareInfo> prepareMap  = new HashMap<>();

  public PrepareStmtContext(MycatSession mycat) {
    this.mycat = mycat;
  }


  public int getNumOfParams(long statementId){
    PrepareInfo prepareInfo = prepareMap.get(statementId);
    return  prepareInfo.getNumOfParams();
  }

  public void newReadyPrepareStmt(String sql, String dataNode, MySQLDataSourceQuery query) {
    final long currentStmtId = stmtId++;
    ProxyRuntime runtime = mycat.getRuntime();
    MySQLDataNode node = runtime.getDataNodeByName(dataNode);
    MySQLReplica replica = (MySQLReplica) node.getReplica();



    replica.getMySQLSessionByBalance(query.isRunOnMaster(), query.getStrategy(),
        null, new SessionCallBack<MySQLClientSession>() {
          @Override
          public void onSession(MySQLClientSession session, Object sender, Object attr) {
            mycat.getMycatReactorThread().getMySQLSessionManager().addIdleSession(session);
            PrepareInfo prepareInfo = new PrepareInfo(currentStmtId, sql, dataNode,
                session.getDatasource(), mycat,
                mycat.getMycatReactorThread().getMySQLSessionManager());
            prepareInfo.getPrepareSession(new PrepareSessionCallback() {
              @Override
              public void onPrepare(long actualStatementId, MySQLClientSession session) {

              }

              @Override
              public void onException(Exception exception, Object sender, Object attr) {

              }

              @Override
              public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                  MySQLClientSession mysql, Object sender, Object attr) {

              }
            },true);
            prepareMap.put(currentStmtId,prepareInfo);
          }

          @Override
          public void onException(Exception exception, Object sender, Object attr) {
            mycat.setLastMessage(exception.toString());
            mycat.writeErrorEndPacket();
          }
        });
  }

  public void execute(long statementId, byte flags, int numParams,
      byte[] rest) {
    PrepareInfo prepareInfo = prepareMap.get(statementId);
    prepareInfo.execute(flags, numParams, rest);

  }
  public void reset(long statementId) {
    PrepareInfo prepareInfo = prepareMap.get(statementId);
    prepareInfo.reset();
  }

  public void close(long statementId) {
    PrepareInfo prepareInfo = prepareMap.get(statementId);
    prepareInfo.close();
  }
  public void fetch(long statementId,long numOfRows) {
    PrepareInfo prepareInfo = prepareMap.get(statementId);
    prepareInfo.fetch(numOfRows);
  }

  public void appendLongData(long statementId, int paramId, byte[] data) {
    PrepareInfo prepareInfo = this.prepareMap.get(statementId);
    prepareInfo.appendLongData(paramId,data);
  }
}