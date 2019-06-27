package io.mycat.command.prepareStatement;

import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
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

  public void newReadyPrepareStmt(String sql, String dataNode, boolean runOnMaster, LoadBalanceStrategy strategy) {
    final long currentStmtId = stmtId++;
    PrepareInfo prepareInfo = new PrepareInfo(currentStmtId, sql,mycat,
        mycat.getIOThread().getMySQLSessionManager());
    prepareInfo.getPrepareSession(dataNode,runOnMaster,strategy,new PrepareSessionCallback() {
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

  public void execute(long statementId, byte flags, int numParams,
      byte[] rest,String dataNode,boolean runOnMaster, LoadBalanceStrategy strategy) {
    PrepareInfo prepareInfo = prepareMap.get(statementId);
    prepareInfo.execute(flags, numParams, rest,dataNode,runOnMaster,strategy);

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