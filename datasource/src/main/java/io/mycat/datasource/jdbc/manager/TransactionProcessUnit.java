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
import java.util.concurrent.atomic.AtomicInteger;

public final class TransactionProcessUnit extends SessionThread {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnit.class);
  private final LinkedTransferQueue<TransactionProcessTask> blockingDeque = new LinkedTransferQueue<>();//todo optimization
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

  public void run(TransactionProcessKey key, TransactionProcessTask processTask) {
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
    blockingDeque.offer(processTask);
  }

  @Override
  public void run() {
    try {
      while (!isInterrupted()) {
        TransactionProcessTask poll = null;
        try {
          poll = blockingDeque.take();
        } catch (InterruptedException e) {
          LOGGER.error("", e);
        }
        if (poll != null) {
          this.startTime = System.currentTimeMillis();
          try {
            poll.accept(this.key, transactionSession);
          } catch (Exception e) {
            LOGGER.error("", e);
          }
          recycleTransactionThread();
        }
      }
    } catch (Exception e) {
      LOGGER.error("", e);
    }
  }

  public void recycleTransactionThread() {
    this.getTransactionSession().afterDoAction();
    if (!this.getTransactionSession().isInTransaction()) {
      manager.map.remove(this.key);
      this.key = null;
      if (!manager.idleList.offer(this)) {
        close();
        decThreadCount();
        manager.allSession.remove(this);
      }
    }
  }

  private void decThreadCount() {
    AtomicInteger threadCounter = manager.threadCounter;
    threadCounter.updateAndGet(operand -> {
      if (operand > 0) {
        return --operand;
      } else {
        return 0;
      }
    });
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