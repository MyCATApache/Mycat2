package io.mycat.datasource.jdbc.response;

import java.sql.Statement;

public class JDBCOkResponse implements JDBCResponse {

  private final Statement statement;
  private final int updateCount;
  private final long lastInsertId;

  public JDBCOkResponse(Statement statement, int updateCount, long lastInsertId) {
    this.statement = statement;
    this.updateCount = updateCount;
    this.lastInsertId = lastInsertId;
  }
}