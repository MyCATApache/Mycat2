package io.mycat.datasource.jdbc.connection;

import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import java.sql.Connection;

public interface ConnectionManager {

  Connection getConnection(JdbcDataSource key) throws Exception;

  void closeConnection(Connection connection) throws Exception;
}