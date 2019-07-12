package io.mycat.datasource.jdbc.response;

import java.sql.SQLException;

public class JDBCErrorResponse implements JDBCResponse {

  public JDBCErrorResponse(SQLException e) {

  }
}