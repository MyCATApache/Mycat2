package io.mycat.datasource.jdbc.datasource;

import java.sql.Connection;

public interface ConnectionManager {

  Connection getConnection(JdbcDataSource key) throws Exception;

  void closeConnection(JdbcDataSource key, Connection connection) throws Exception;
}