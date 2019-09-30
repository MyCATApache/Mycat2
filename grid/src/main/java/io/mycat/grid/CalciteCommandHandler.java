package io.mycat.grid;

import io.mycat.beans.resultset.MycatResponse;
import io.mycat.command.AbstractCommandHandler;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.SQLExecuterWriter;
import io.mycat.proxy.session.MycatSession;
import java.util.Map;

public class CalciteCommandHandler extends AbstractCommandHandler {

  private final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(GridProxyCommandHandler.class);
  private final static String UNSUPPORT = "unsupport!";
  ExecuterBuilder executionPlan;

  @Override
  public void initRuntime(MycatSession session, ProxyRuntime runtime) {
    Map<String, Object> defContext = runtime.getDefContext();
    GRuntime jdbcRuntime = (GRuntime) defContext.get("gridRuntime");
    this.executionPlan = new CalciteExecuterBuilderImpl(session, jdbcRuntime);
  }

  @Override
  public void handleQuery(byte[] sqlBytes, MycatSession session) {
    String sql = new String(sqlBytes);
    LOGGER.info(sql);
    MycatResponse[] executer = executionPlan.generate(sqlBytes);
    SQLExecuterWriter.writeToMycatSession(session, executer);
  }

  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession session) {
    session.setLastMessage(UNSUPPORT);
    session.writeErrorEndPacket();
  }

  @Override
  public void handleContentOfFilenameEmptyOk(MycatSession session) {
    session.setLastMessage(UNSUPPORT);
    session.writeErrorEndPacket();
  }


  @Override
  public void handlePrepareStatement(byte[] sql, MycatSession session) {
    session.setLastMessage(UNSUPPORT);
    session.writeErrorEndPacket();
  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession session) {
    session.setLastMessage(UNSUPPORT);
    session.writeErrorEndPacket();
  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams, byte[] rest, MycatSession session) {
    session.setLastMessage(UNSUPPORT);
    session.writeErrorEndPacket();
  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {
    session.setLastMessage(UNSUPPORT);
    session.writeErrorEndPacket();
  }

  @Override
  public void handlePrepareStatementFetch(long statementId, long row,
      MycatSession mycat) {
    mycat.setLastMessage(UNSUPPORT);
    mycat.writeErrorEndPacket();
  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession session) {
    session.setLastMessage(UNSUPPORT);
    session.writeErrorEndPacket();
  }

  @Override
  public int getNumParamsByStatementId(long statementId, MycatSession mycat) {
    return 0;
  }
}