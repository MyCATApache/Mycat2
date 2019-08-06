package io.mycat.datasource.jdbc.transaction;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.datasource.jdbc.connection.AutocommitConnection;
import io.mycat.datasource.jdbc.connection.LocalTransactionConnection;
import io.mycat.datasource.jdbc.connection.XATransactionConnection;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.LinkedTransferQueue;

public final class TransactionProcessUnit extends SessionThread {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnit.class);
  private volatile long startTime;
  private final LinkedTransferQueue<Runnable> blockingDeque = new LinkedTransferQueue<>();
  private final TransactionSession transactionSession;
  private final GridRuntime runtime;

  public TransactionProcessUnit(GridRuntime runtime) {
    boolean jta = runtime.getDatasourceProvider().isJTA();
    this.transactionSession = !jta ? new LocalTransactionSessionImpl(this)
        : new JTATransactionSessionImpl(
            runtime.getDatasourceProvider().createUserTransaction(), this);
    this.runtime = runtime;
  }

  public void run(Runnable runnale) {
    blockingDeque.offer(runnale);
  }

  @Override
  public void run() {
    try {
      while (!isInterrupted()) {
        Runnable poll = null;
        try {
          poll = blockingDeque.take();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (poll != null) {
          this.startTime = System.currentTimeMillis();
          try {
            poll.run();
          } catch (Exception e) {
            LOGGER.error("", e);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("---------------------------------------------");
  }

  public long getStartTime() {
    return startTime;
  }

  public void close() {
    super.close();
//    interrupt();
//    blockingDeque.add(END);
  }

  public AutocommitConnection getAutocommitConnection(JdbcDataSource dataSource) {
    return new AutocommitConnection(runtime.getConnection(dataSource), dataSource);
  }

  public LocalTransactionConnection getLocalTransactionConnection(JdbcDataSource dataSource,
      int transactionIsolation) {
    Connection connection = runtime.getConnection(dataSource);
    return new LocalTransactionConnection(connection, dataSource, transactionIsolation);
  }

  public XATransactionConnection getXATransactionConnection(JdbcDataSource dataSource,
      int transactionIsolation) {
    Connection connection = runtime.getConnection(dataSource);
    try {
      if (connection.isClosed()) {
        throw new MycatException("");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return new XATransactionConnection(connection, dataSource, transactionIsolation);
  }

  public TransactionSession getTransactionSession() {
    return transactionSession;
  }

  public GridRuntime getRuntime() {
    return runtime;
  }
}