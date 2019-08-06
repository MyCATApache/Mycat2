package io.mycat.datasource.jdbc.connection;

import io.mycat.datasource.jdbc.JdbcDataSource;
import java.sql.Connection;

public class AutocommitConnection extends AbsractConnection {

  public AutocommitConnection(Connection connection, JdbcDataSource jdbcDataSource) {
    super(connection, jdbcDataSource);
  }
}