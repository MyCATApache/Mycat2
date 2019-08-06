package io.mycat.datasource.jdbc.connection;

import io.mycat.datasource.jdbc.JdbcDataSource;
import java.sql.Connection;

public class AutocommitConnection extends AbsractConnection {

  public AutocommitConnection(Connection connection, JdbcDataSource jdbcDataSource,
      ConnectionManager connectionManager) {
    super(connection, jdbcDataSource, connectionManager);
  }

  public Connection getConnection() {
    return connection;
  }
}