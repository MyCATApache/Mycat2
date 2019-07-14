package io.mycat.grid;

import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.proxy.session.MycatSession;

public class ExecutionPlan {

  final MycatSession session;
  private JdbcRuntime jdbcRuntime;

  public ExecutionPlan(MycatSession session, JdbcRuntime jdbcRuntime) {
    this.session = session;
    this.jdbcRuntime = jdbcRuntime;
  }

  public SQLExecuter[] generate(byte[] sql) {
    return new SQLExecuter[]{new SQLExecuter(new String(sql), session, jdbcRuntime)};
  }
}