package io.mycat.proxy.command;

/**
 * @author jamie12221
 * @date 2019-05-12 21:30
 * 实现mysql服务器 预处理相关处理
 **/
public interface PrepareStatementHandler {

  void handlePrepareStatement(byte[] sql, MycatSessionView session);

  void handlePrepareStatementLongdata(long statementId, long paramId, byte[] data,
      MycatSessionView session);

  void handlePrepareStatementExecute(long statementId, byte flags, int numParams, byte[] nullMap,
      boolean newParamsBound
      , byte[] typeList, byte[] fieldList, MycatSessionView session);

  void handlePrepareStatementClose(long statementId, MycatSessionView session);

  void handlePrepareStatementReset(long statementId, MycatSessionView session);

  interface PrepareStatementSession {

    int getNumParamsByStatementId(long statementId);
  }
}
