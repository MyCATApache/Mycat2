package io.mycat.datasource.jdbc.thread;

import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadPool;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.DsConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.TransactionSession;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.proxy.session.Session;

public class GThread extends BindThread implements SessionThread {

  protected final GRuntime runtime;
  protected final TransactionSession transactionSession;
  protected Session session;

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

  @Override
  public Session getCurSession() {
    return session;
  }

  @Override
  public void setCurSession(Session session) {
    this.session = session;
  }
}