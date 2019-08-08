package io.mycat.datasource.jdbc.thread;

import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadPool;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.connection.DsConnection;
import io.mycat.datasource.jdbc.connection.TransactionSession;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;

public class GThread extends BindThread {

  protected final GRuntime runtime;
  protected final TransactionSession transactionSession;

  public GThread(GRuntime runtime, BindThreadPool manager) {
    super(manager);
    this.transactionSession = runtime.createTransactionSession(this);
    this.runtime = runtime;
  }

  @Override
  protected boolean continueBind() {
    return transactionSession.isInTransaction();
  }

  public DsConnection getConnection(JdbcDataSource dataSource,
      int transactionIsolation) {
    return dataSource.getReplica().getConnection(dataSource, true, transactionIsolation);
  }

  public DsConnection getConnection(JdbcDataSource dataSource, boolean autocommit,
      int transactionIsolation) {
    return dataSource.getReplica().getConnection(dataSource, autocommit, transactionIsolation);
  }

  public TransactionSession getTransactionSession() {
    return transactionSession;
  }
}