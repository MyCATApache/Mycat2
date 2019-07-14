package io.mycat.grid;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.JdbcSession;
import io.mycat.datasource.jdbc.MycatResponse;
import io.mycat.datasource.jdbc.MycatResultSetResponseImpl;
import io.mycat.proxy.session.MycatSession;

public class SQLExecuter {

  private final String sql;
  private final MycatSession session;
  private JdbcRuntime jdbcRuntime;

  public SQLExecuter(String sql, MycatSession session,
      JdbcRuntime jdbcRuntime) {
    this.sql = sql;
    this.session = session;
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
    return session;
  }
}