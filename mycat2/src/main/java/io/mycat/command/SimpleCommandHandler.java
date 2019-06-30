package io.mycat.command;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouterConfig;
import io.mycat.sqlparser.util.BufferSQLContext;
import io.mycat.sqlparser.util.BufferSQLParser;

public class SimpleCommandHandler extends AbstractCommandHandler {

  static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(SimpleCommandHandler.class);
  final static String UNSUPPORT_MESSAGE = "mycat default command handler is unsupport this command";
  BufferSQLContext sqlContext;
  BufferSQLParser sqlParser;
  MycatRouterConfig config;

  @Override
  public void initRuntime(MycatSession session, ProxyRuntime runtime) {
    sqlContext = new BufferSQLContext();
    sqlParser = new BufferSQLParser();
    config = (MycatRouterConfig) runtime.getDefContext().get("routeConfig");
  }

  @Override
  public void handleQuery(byte[] sql, MycatSession session) {
    boolean simpleSelect = false;
    try {
      sqlParser.parse(sql, sqlContext);
      simpleSelect = sqlContext.isSimpleSelect();
    } catch (Exception e) {
      LOGGER.warn("", e);
    }
    try {
      MycatSchema schema = config.getSchemaOrDefaultBySchemaName(session.getSchema());
      session.useSchema(schema.getSchemaName());
      MySQLDataSourceQuery query = new MySQLDataSourceQuery();
      query.setRunOnMaster(!simpleSelect);
      MySQLTaskUtil.proxyBackend(session,
          MySQLPacketUtil.generateComQueryPayload(sql),
          schema.getDefaultDataNode(), query, ResponseType.QUERY);
    } catch (Exception e) {
      LOGGER.error("", e);
      session.setLastMessage(e);
      session.writeErrorEndPacket();
    }
  }

  void error(MycatSession session) {
    LOGGER.error(UNSUPPORT_MESSAGE);
    session.setLastMessage(new Throwable());
    session.writeErrorEndPacket();
  }

  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession session) {
    error(session);
  }

  @Override
  public void handleContentOfFilenameEmptyOk(MycatSession session) {
    error(session);
  }

  @Override
  public void handlePrepareStatement(byte[] sql, MycatSession session) {
    error(session);
  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession session) {

  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams, byte[] rest, MycatSession session) {
    error(session);
  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {
    error(session);
  }

  @Override
  public void handlePrepareStatementFetch(long statementId, long row, MycatSession session) {
    error(session);
  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession session) {
    error(session);
  }

  @Override
  public int getNumParamsByStatementId(long statementId, MycatSession session) {
    return 0;
  }
}