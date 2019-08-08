package io.mycat.datasource.jdbc.connection;

import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;

public interface DsConnection {

  MycatUpdateResponse executeUpdate(String sql, boolean needGeneratedKeys);

  JdbcRowBaseIteratorImpl executeQuery(String sql);

  void close();

  void setTransactionIsolation(int transactionIsolation);

  JdbcDataSource getDataSource();

  boolean isClosed();
}