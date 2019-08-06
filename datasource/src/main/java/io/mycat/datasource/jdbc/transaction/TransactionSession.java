package io.mycat.datasource.jdbc.transaction;

import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.datasource.jdbc.connection.AbsractConnection;

public interface TransactionSession {

  void setTransactionIsolation(int transactionIsolation);

  void begin();

  void commit();

  void rollback();

  boolean isInTransaction();

  void beforeDoAction();

  void afterDoAction();

  void setAutocommit(boolean autocommit);

  AbsractConnection getConnection(JdbcDataSource jdbcDataSource);

}