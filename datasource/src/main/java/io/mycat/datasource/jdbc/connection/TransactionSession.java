package io.mycat.datasource.jdbc.connection;

import io.mycat.datasource.jdbc.datasource.JdbcDataSource;

public interface TransactionSession {

  void setTransactionIsolation(int transactionIsolation);

  void begin();

  void commit();

  void rollback();

  boolean isInTransaction();

  void beforeDoAction();

  void afterDoAction();

  void setAutocommit(boolean autocommit);

  DsConnection getConnection(JdbcDataSource jdbcDataSource);

}