package io.mycat.command;


import io.mycat.proxy.session.MycatSession;

/**
 * @author jamie12221
 *  date 2019-05-12 21:30
 * 实现mysql服务器 预处理相关处理
 **/
public interface PrepareStatementParserHelper {

  void handlePrepareStatement(byte[] sql, MycatSession session);

  void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession session);

  void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags, int numParams,
      byte[] nullMap,
      boolean newParamsBound, byte[] typeList, byte[] fieldList, MycatSession session);

  void handlePrepareStatementClose(long statementId, MycatSession session);

  void handlePrepareStatementReset(long statementId, MycatSession session);

  interface PrepareStatementSession {

    int getNumParamsByStatementId(long statementId);
  }
}
