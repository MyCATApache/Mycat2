package io.mycat.datasource.jdbc.datasource;

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