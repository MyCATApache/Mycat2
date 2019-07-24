package io.mycat.command;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.command.loaddata.LoaddataContext;
import io.mycat.command.prepareStatement.PrepareStmtContext;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatProxyStaticAnnotation;
import io.mycat.router.MycatRouterConfig;
import io.mycat.sqlparser.util.BufferSQLContext;
import io.mycat.sqlparser.util.BufferSQLParser;
import java.util.Objects;

public class ReadAndWriteSeparationHandler extends AbstractCommandHandler {

  static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(ReadAndWriteSeparationHandler.class);
  final static String UNSUPPORT_MESSAGE = "mycat default command handler is unsupport this command";
  BufferSQLContext sqlContext;
  BufferSQLParser sqlParser;
  MycatRouterConfig config;
  private PrepareStmtContext prepareContext;
  private final LoaddataContext loadDataContext = new LoaddataContext();
  private final MycatProxyStaticAnnotation map = new MycatProxyStaticAnnotation();

  @Override
  public void initRuntime(MycatSession session, ProxyRuntime runtime) {
    this.sqlContext = new BufferSQLContext();
    this.sqlParser = new BufferSQLParser();
    this.prepareContext = new PrepareStmtContext(session);
    this.config = (MycatRouterConfig) runtime.getDefContext().get("routeConfig");
    MycatSchema schema = config.getDefaultSchema();
    Objects.requireNonNull(schema, "please config default schema");
    session.setSchema(schema.getSchemaName());
    Objects.requireNonNull(schema.getDefaultDataNode(), "please config default dataNode");
    session.switchDataNode(schema.getDefaultDataNode());
  }

  @Override
  public void handleQuery(byte[] sql, MycatSession session) {
    boolean simpleSelect = false;
    MySQLDataSourceQuery query = new MySQLDataSourceQuery();
    try {
      sqlParser.parse(sql, sqlContext);
      simpleSelect = sqlContext.isSimpleSelect();
      MycatProxyStaticAnnotation sa = sqlContext.getStaticAnnotation()
          .toMapAndClear(map);
      String balance = sa.getBalance();
      Boolean runOnMaster = sa.getRunOnMaster();
      query.setRunOnMaster(!simpleSelect);
      if (balance!=null){
        query.setStrategy(session.getRuntime().getLoadBalanceByBalanceName(balance));
      }
      if (runOnMaster!=null){
        query.setRunOnMaster(runOnMaster);
      }
      switch (sqlContext.getSQLType()){
        case BufferSQLContext.SET_TRANSACTION_SQL:{
          session.setIsolation(sqlContext.getIsolation());
          break;
        }
        case BufferSQLContext.SET_AUTOCOMMIT_SQL:{
          session.setAutoCommit(sqlContext.getAutocommit());
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.error("", e);
    }
    try {
      MySQLTaskUtil.proxyBackend(session, new String(sql),
          session.getDataNode(), query);
    } catch (Exception e) {
      LOGGER.error("", e);
      session.setLastMessage(e);
      session.writeErrorEndPacket();
    }finally {
      map.clear();
    }
  }

  void error(MycatSession session) {
    LOGGER.error(UNSUPPORT_MESSAGE);
    session.setLastMessage(new Throwable());
    session.writeErrorEndPacket();
  }

  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession session) {
    loadDataContext.append(sql);
  }

  @Override
  public void handleContentOfFilenameEmptyOk(MycatSession session) {
    loadDataContext.proxy(session);
  }

  @Override
  public void handlePrepareStatement(byte[] sql, MycatSession session) {
    prepareContext.newReadyPrepareStmt(new String(sql), session.getDataNode(), true, null);
  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession session) {
    prepareContext.appendLongData(statementId, paramId, data);

  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams, byte[] rest, MycatSession session) {
    prepareContext.execute(statementId, flags, numParams, rest, session.getDataNode(), true, null);
  }

  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {
    prepareContext.close(statementId);
  }

  @Override
  public void handlePrepareStatementFetch(long statementId, long row, MycatSession session) {
    prepareContext.fetch(statementId, row);
  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession session) {
    prepareContext.reset(statementId);
  }

  @Override
  public int getNumParamsByStatementId(long statementId, MycatSession session) {
    return prepareContext.getNumOfParams(statementId);
  }
}