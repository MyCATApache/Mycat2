package io.mycat.grid;

import static io.mycat.sqlparser.util.BufferSQLContext.DELETE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.INSERT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_VARIABLES_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_WARNINGS;
import static io.mycat.sqlparser.util.BufferSQLContext.UPDATE_SQL;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.DataNodeSession;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.MycatResponse;
import io.mycat.datasource.jdbc.MycatResultSetResponse;
import io.mycat.datasource.jdbc.MycatUpdateResponse;
import io.mycat.proxy.session.MycatSession;
import io.mycat.sqlparser.util.BufferSQLContext;
import io.mycat.sqlparser.util.BufferSQLParser;

public class ExecutionPlanBuilder {

  final MycatSession mycat;
  private final BufferSQLParser parser;
  private final BufferSQLContext sqlContext;
  private GridRuntime jdbcRuntime;
  private DataNodeSession dataNodeSession;

  public ExecutionPlanBuilder(MycatSession session, GridRuntime jdbcRuntime) {
    this.mycat = session;
    this.jdbcRuntime = jdbcRuntime;
    this.dataNodeSession = new DataNodeSession(jdbcRuntime);
    this.parser = new BufferSQLParser();
    this.sqlContext = new BufferSQLContext();
  }

  public SQLExecuter[] generate(byte[] sqlBytes) {
    String sql = new String(sqlBytes);
    parser.parse(sqlBytes, sqlContext);
    byte sqlType =sqlContext.getSQLType()==0? sqlContext.getCurSQLType():sqlContext.getSQLType();
    switch (sqlType) {
      case BufferSQLContext.BEGIN_SQL:
      case BufferSQLContext.START_SQL:
      case BufferSQLContext.START_TRANSACTION_SQL: {
        dataNodeSession.startTransaction();
        return new SQLExecuter[]{new UpdateResponseExecuter(mycat)};
      }
      case BufferSQLContext.COMMIT_SQL: {
        dataNodeSession.commit();
        return new SQLExecuter[]{new UpdateResponseExecuter(mycat)};
      }
      case BufferSQLContext.ROLLBACK_SQL: {
        dataNodeSession.rollback();
        return new SQLExecuter[]{new UpdateResponseExecuter(mycat)};
      }
      case SET_TRANSACTION_SQL: {
        MySQLIsolation isolation = sqlContext.getIsolation();
        if (isolation == null) {
          throw new MycatException("unsupport!");
        }
        dataNodeSession.setTransactionIsolation(isolation);
        return new SQLExecuter[]{new UpdateResponseExecuter(mycat)};
      }

      case SHOW_DB_SQL:
      case SHOW_SQL:
      case SHOW_TB_SQL:
      case SHOW_WARNINGS:
      case SHOW_VARIABLES_SQL: {
        MycatResultSetResponse response = dataNodeSession
            .executeQuery("dn1", sql, false, null);
        return new SQLExecuter[]{() -> response};
      }
      case UPDATE_SQL:
      case INSERT_SQL: {
        MycatUpdateResponse response = dataNodeSession.executeUpdate("dn1", sql, true, null);
        return new SQLExecuter[]{() -> response};
      }
      case DELETE_SQL:{
        MycatUpdateResponse response = dataNodeSession.executeUpdate("dn1", sql, true, null);
        return new SQLExecuter[]{() -> response};
      }
      case SELECT_SQL:{
        if(sqlContext.isSimpleSelect()){
          MycatResultSetResponse response = dataNodeSession
              .executeQuery("dn1", sql, false, null);
          return new SQLExecuter[]{() -> response};
        }else {
          MycatResultSetResponse response = dataNodeSession
              .executeQuery("dn1", sql, false, null);
          return new SQLExecuter[]{() -> response};
        }
      }
      default:
        return new SQLExecuter[]{new UpdateResponseExecuter(mycat)};
    }
  }
}