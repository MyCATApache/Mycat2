package io.mycat.grid;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcSession;
import io.mycat.datasource.jdbc.MycatResponse;
import io.mycat.datasource.jdbc.MycatResultSetResponseImpl;
import io.mycat.proxy.session.MycatSession;

public class SQLExecuterImpl implements SQLExecuter{

  private final String sql;
  private final MycatSession mycat;
  private GridRuntime jdbcRuntime;

  public SQLExecuterImpl(String sql, MycatSession session,
      GridRuntime jdbcRuntime) {
    this.sql = sql;
    this.mycat = session;
    this.jdbcRuntime = jdbcRuntime;
  }

  public MycatResponse execute() throws Exception {
    JdbcSession jdbcSession = jdbcRuntime
        .getJdbcSessionByDataNodeName("dn1", MySQLIsolation.READ_UNCOMMITTED,
            MySQLAutoCommit.ON, null);
    MycatResultSetResponseImpl mycatResultSetResponse = new MycatResultSetResponseImpl(jdbcSession,
        jdbcSession.executeQuery(sql));
    return mycatResultSetResponse;
  }

  public MycatSession getMycatSession() {
    return mycat;
  }
}