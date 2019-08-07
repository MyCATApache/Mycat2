package io.mycat.datasource.jdbc.session;

import io.mycat.datasource.jdbc.connection.AbsractConnection;
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

  AbsractConnection getConnection(JdbcDataSource jdbcDataSource);

}