package io.mycat.datasource.jdbc.session;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.connection.AbsractConnection;
import io.mycat.datasource.jdbc.connection.XATransactionConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.manager.TransactionProcessUnit;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

public class JTATransactionSessionImpl implements TransactionSession {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(JTATransactionSessionImpl.class);
  private final UserTransaction userTransaction;
  private final TransactionProcessUnit transactionProcessUnit;
  private final Map<JdbcDataSource, XATransactionConnection> connectionMap = new HashMap<>();
  private boolean autocommit = true;
  private int transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;

  public JTATransactionSessionImpl(UserTransaction userTransaction,
      TransactionProcessUnit transactionProcessUnit) {
    this.userTransaction = userTransaction;
    this.transactionProcessUnit = transactionProcessUnit;
  }

  @Override
  public void setTransactionIsolation(int transactionIsolation) {
    this.transactionIsolation = transactionIsolation;
  }

  @Override
  public void begin() {

    connectionMap.values().forEach(c -> c.close());
    connectionMap.clear();
    try {
      LOGGER.debug("{} begin", userTransaction);
      userTransaction.begin();
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  public AbsractConnection getConnection(JdbcDataSource jdbcDataSource) {
    beforeDoAction();
    XATransactionConnection connection;
    do {
      connection = connectionMap.compute(jdbcDataSource,
          new BiFunction<JdbcDataSource, XATransactionConnection, XATransactionConnection>() {
            @Override
            public XATransactionConnection apply(JdbcDataSource dataSource,
                XATransactionConnection absractConnection) {
              if (absractConnection != null) {
                return absractConnection;
              } else {
                return transactionProcessUnit
                    .getXATransactionConnection(jdbcDataSource, transactionIsolation);
              }
            }
          });
    } while (connection.isClosed());
    return connection;
  }

  @Override
  public void commit() {
    try {
      userTransaction.commit();
    } catch (Exception e) {
      LOGGER.error("", e);
      throw new MycatException(e);
    }
    afterDoAction();
  }

  @Override
  public void rollback() {
    try {
      userTransaction.setRollbackOnly();
    } catch (Exception e) {
      throw new MycatException(e);
    }
    afterDoAction();
  }

  @Override
  public boolean isInTransaction() {
    try {
      int status = userTransaction.getStatus();
      switch (status) {
        case Status.STATUS_NO_TRANSACTION:
        case Status.STATUS_UNKNOWN:
        case Status.STATUS_ACTIVE:
        case Status.STATUS_MARKED_ROLLBACK:
        case Status.STATUS_COMMITTED:
        case Status.STATUS_ROLLEDBACK:
          return false;
        default:
          return true;
      }
    } catch (SystemException e) {
      throw new MycatException(e);
    }
  }

  @Override
  public void beforeDoAction() {
    try {
      if (!this.autocommit && !isInTransaction()) {
        begin();
      }

      System.out.println("--------------------------------------------------------------------");
      System.out.println(userTransaction.getStatus());
    } catch (SystemException e) {
      throw new MycatException(e);
    }
  }

  @Override
  public void afterDoAction() {
    if (!isInTransaction()) {
      close();
    }
  }

  @Override
  public void setAutocommit(boolean autocommit) {
    this.autocommit = autocommit;
  }


  public void close() {
    connectionMap.values().forEach(AbsractConnection::close);
    connectionMap.clear();
  }
}