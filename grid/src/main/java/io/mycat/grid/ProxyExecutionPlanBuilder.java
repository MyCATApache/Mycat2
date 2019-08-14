package io.mycat.grid;

import static io.mycat.sqlparser.util.BufferSQLContext.DELETE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.INSERT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_FOR_UPDATE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_VARIABLES_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_WARNINGS;
import static io.mycat.sqlparser.util.BufferSQLContext.UPDATE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.USE_SQL;

import io.mycat.MycatException;
import io.mycat.beans.MySQLServerStatus;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.SQLExecuter;
import io.mycat.config.schema.SchemaType;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ProxyRouteResult;
import io.mycat.router.util.RouterUtil;
import io.mycat.sqlparser.util.BufferSQLContext;
import io.mycat.sqlparser.util.BufferSQLParser;
import java.util.Objects;

public class ProxyExecutionPlanBuilder implements ExecuterBuilder {

  private static final MycatLogger IGNORED_SQL_LOGGER = MycatLoggerFactory
      .getLogger("IGNORED_SQL_LOGGER");
  final MycatSession mycat;
  private final BufferSQLParser parser;
  private final BufferSQLContext sqlContext;
  private final MycatRouter router;
  private GRuntime jdbcRuntime;

  public ProxyExecutionPlanBuilder(MycatSession mycat) {
    this.mycat = mycat;
    this.jdbcRuntime = GRuntime.INSTACNE;
    this.parser = new BufferSQLParser();
    this.sqlContext = new BufferSQLContext();
    MycatRouterConfig routerConfig = (MycatRouterConfig) jdbcRuntime.getDefContext()
        .get("routerConfig");
    Objects.requireNonNull(routerConfig);
    this.router = new MycatRouter(routerConfig);
  }

  public SQLExecuter[] generate(byte[] sqlBytes) {
    TransactionSession transactionSession = ((GThread) Thread.currentThread())
        .getTransactionSession();
    MycatSchema schema = null;
    parser.parse(sqlBytes, sqlContext);
    String orgin = new String(sqlBytes);
    String sql;
    boolean b = sqlContext.getSchemaCount() > 0;
    if (b) {
      String schemaName = sqlContext.getSchemaName(0);
      sql = RouterUtil.removeSchema(orgin, schemaName).trim();
      schema = router
          .getSchemaOrDefaultBySchemaName(schemaName);
    } else {
      sql = orgin;
    }
    if (schema == null) {
      schema = router.getSchemaOrDefaultBySchemaName(mycat.getSchema());
    }
    byte sqlType =
        sqlContext.getSQLType() == 0 ? sqlContext.getCurSQLType() : sqlContext.getSQLType();

    MycatMonitor.onOrginSQL(mycat, orgin);

    if (sql.contains("set autocommit=0")) {
      return begin(transactionSession);
    }
    switch (sqlType) {
      case BufferSQLContext.BEGIN_SQL:
      case BufferSQLContext.START_SQL:
      case BufferSQLContext.START_TRANSACTION_SQL: {
        return begin(transactionSession);
      }
      case BufferSQLContext.COMMIT_SQL: {
        return commit(transactionSession);
      }
      case BufferSQLContext.SET_AUTOCOMMIT_SQL: {
        transactionSession.setAutocommit(sqlContext.isAutocommit());
        mycat.setAutoCommit(sqlContext.isAutocommit());
        return responseOk();
      }
      case BufferSQLContext.ROLLBACK_SQL: {
        return rollback(transactionSession);
      }
      case SET_TRANSACTION_SQL: {
        return setTranscation(transactionSession);
      }
      case SHOW_DB_SQL:
        return new SQLExecuter[]{
            MycatRouterResponse.showDb(mycat, router.getConfig().getSchemaList())};
      case SHOW_TB_SQL:
        return new SQLExecuter[]{
            MycatRouterResponse.showTable(router, mycat, schema.getSchemaName())};
      case SHOW_WARNINGS:
        return new SQLExecuter[]{
            MycatRouterResponse.showWarnnings(mycat)};
      case SHOW_SQL:
        return responseOk();
      case SHOW_VARIABLES_SQL: {
        return directSQL(schema, sql);
      }
      case USE_SQL: {
        return useSchema();
      }
      case UPDATE_SQL:
      case INSERT_SQL:
      case DELETE_SQL:
      case SELECT_FOR_UPDATE_SQL:
      case SELECT_SQL: {
        if (router.existTable(schema, sqlContext.getTableName(0))) {
          return new SQLExecuter[]{
              execute(sqlType, this.router.enterRoute(schema, sqlContext, sql))};
        } else if (SELECT_SQL == sqlType || SELECT_FOR_UPDATE_SQL == sqlType
            || schema.getSchemaType() == SchemaType.DB_IN_ONE_SERVER) {
          return directSQL(schema, sql);
        }
      }
      default:
        IGNORED_SQL_LOGGER.warn("ignore:{}", sql);
        return responseOk();
    }
  }

  private SQLExecuter[] directSQL(MycatSchema schema, String sql) {
    MycatResultSetResponse response = TransactionSessionUtil
        .executeQuery(router.getRandomDataNode(schema), sql, true, null);
    return new SQLExecuter[]{() -> response};
  }

  private SQLExecuter[] useSchema() {
    String schemaName = sqlContext.getSchemaName(0);
    mycat.useSchema(schemaName);
    return responseOk();
  }

  private SQLExecuter[] setTranscation(TransactionSession transactionSession) {
    MySQLIsolation isolation = sqlContext.getIsolation();
    if (isolation == null) {
      throw new MycatException("unsupport!");
    }
    transactionSession.setTransactionIsolation(isolation.getJdbcValue());
    mycat.setIsolation(isolation);
    return responseOk();
  }

  private SQLExecuter[] begin(TransactionSession transactionSession) {
    MySQLServerStatus serverStatus = mycat.getServerStatus();
    serverStatus.addServerStatusFlag(MySQLServerStatusFlags.IN_TRANSACTION);
    transactionSession.begin();
    return responseOk();
  }

  private SQLExecuter[] commit(TransactionSession transactionSession) {
    MySQLServerStatus serverStatus = mycat.getServerStatus();
    serverStatus.removeServerStatusFlag(MySQLServerStatusFlags.IN_TRANSACTION);
    transactionSession.commit();
    return responseOk();
  }

  private SQLExecuter[] rollback(TransactionSession transactionSession) {
    transactionSession.rollback();
    MySQLServerStatus serverStatus = mycat.getServerStatus();
    serverStatus.removeServerStatusFlag(MySQLServerStatusFlags.IN_TRANSACTION);
    return responseOk();
  }

  private SQLExecuter execute(byte sqlType, ProxyRouteResult routeResult) {
    String dataNode = routeResult.getDataNode();
    String sql = routeResult.getSql();
    MycatMonitor.onRouteSQL(mycat, dataNode, sql);
    String balance = routeResult.getBalance();
    LoadBalanceStrategy loadBalanceByBalance = PlugRuntime.INSTCANE
        .getLoadBalanceByBalanceName(balance);
    switch (sqlType) {
      case UPDATE_SQL:
      case DELETE_SQL:
      case INSERT_SQL: {
        MycatUpdateResponse response = TransactionSessionUtil
            .executeUpdate(dataNode, sql, sqlType == INSERT_SQL, true, loadBalanceByBalance);
        return () -> response;
      }
      case SELECT_FOR_UPDATE_SQL:
      case SELECT_SQL: {
        boolean runOnMaster = routeResult.isRunOnMaster(false) || !sqlContext.isSimpleSelect();
        MycatResultSetResponse response = TransactionSessionUtil
            .executeQuery(routeResult.getDataNode(), sql, runOnMaster, loadBalanceByBalance);
        return () -> response;
      }
      default:
    }
    throw new MycatException("unsupportSQL:{}", sql);
  }

  private SQLExecuter[] responseOk() {
    return new SQLExecuter[]{new UpdateResponseExecuter(mycat)};
  }
}