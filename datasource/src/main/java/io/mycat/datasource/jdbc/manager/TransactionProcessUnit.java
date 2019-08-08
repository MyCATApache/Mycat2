package io.mycat.datasource.jdbc.manager;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.connection.AutocommitConnection;
import io.mycat.datasource.jdbc.connection.LocalTransactionConnection;
import io.mycat.datasource.jdbc.connection.XATransactionConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.session.JTATransactionSessionImpl;
import io.mycat.datasource.jdbc.session.LocalTransactionSessionImpl;
import io.mycat.datasource.jdbc.session.TransactionSession;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;
import java.util.Objects;
import java.util.concurrent.LinkedTransferQueue;

public final class TransactionProcessUnit extends SessionThread {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnit.class);
  private final LinkedTransferQueue<TransactionProcessJob> blockingDeque = new LinkedTransferQueue<>();//todo optimization
  private final TransactionProcessUnitManager manager;
  private long startTime;
  private final TransactionSession transactionSession;
  private volatile TransactionProcessKey key;

  public TransactionProcessUnit(GRuntime runtime, TransactionProcessUnitManager manager) {
    boolean jta = runtime.getDatasourceProvider().isJTA();
    this.transactionSession = !jta ? new LocalTransactionSessionImpl(this)
        : new JTATransactionSessionImpl(
            runtime.getDatasourceProvider().createUserTransaction(), this);
    this.manager = manager;
  }

  public void run(TransactionProcessKey key, TransactionProcessJob processTask) {
    Objects.requireNonNull(key);
    if (!blockingDeque.isEmpty()) {
      throw new MycatException("unknown state");
    }
    if (this.key == null) {
      this.key = key;
    } else if (this.key != null && this.key == key) {

    } else {
      throw new MycatException("unknown state");
    }
    if (Thread.currentThread() == processTask) {
      processJob(null, processTask);
    } else {
      blockingDeque.offer(processTask);
    }
  }

  @Override
  public void run() {
    try {
      Exception exception = null;
      TransactionProcessJob poll = null;
      while (!isInterrupted()) {
        exception = null;
        poll = null;
        try {
          poll = blockingDeque.poll(manager.timeout, manager.timeoutUnit);
        } catch (InterruptedException e) {
        }
        if (poll != null) {
          processJob(exception, poll);
          recycleTransactionThread();
        } else {
          manager.tryDecThread();
        }
        /////////////////////////////////
        manager.pollTask();
      }
    } catch (Exception e) {
      LOGGER.error("", e);
    }
  }

  private void processJob(Exception exception, TransactionProcessJob poll) {
    this.startTime = System.currentTimeMillis();
    try {
      poll.accept(this.key, transactionSession);
    } catch (Exception e) {
      exception = e;
      LOGGER.error("", e);
    }

    if (exception != null) {
      try {
        poll.onException(key, exception);
      } catch (Exception e) {
        LOGGER.error("", e);
      }
    }
  }

  public void recycleTransactionThread() {
    this.getTransactionSession().afterDoAction();
    if (!this.getTransactionSession().isInTransaction()) {
      manager.map.remove(this.key);
      this.key = null;
      if (!manager.idleList.offer(this)) {
        close();
        manager.decThreadCount();
        manager.allSession.remove(this);
      }
    }
  }


  public long getStartTime() {
    return startTime;
  }

  public void close() {
    super.close();
    this.interrupt();
  }

  public AutocommitConnection getAutocommitConnection(JdbcDataSource dataSource) {
    return dataSource.getReplica().getAutocomitConnection(dataSource);
  }

  public LocalTransactionConnection getLocalTransactionConnection(JdbcDataSource dataSource,
      int transactionIsolation) {
    return dataSource.getReplica().getLocalTransactionConnection(dataSource, transactionIsolation);
  }

  public XATransactionConnection getXATransactionConnection(JdbcDataSource dataSource,
      int transactionIsolation) {
    return dataSource.getReplica().getXATransactionConnection(dataSource, transactionIsolation);
  }

  public TransactionSession getTransactionSession() {
    return transactionSession;
  }


  public GRuntime getRuntime() {
    return manager.runtime;
  }
}