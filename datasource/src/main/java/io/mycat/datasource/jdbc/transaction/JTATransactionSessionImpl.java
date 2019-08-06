package io.mycat.datasource.jdbc.transaction;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.datasource.jdbc.connection.AbsractConnection;
import io.mycat.datasource.jdbc.connection.XATransactionConnection;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

public class JTATransactionSessionImpl implements TransactionSession {

  private final UserTransaction userTransaction;
  private final TransactionProcessUnit transactionProcessUnit;
  private final Map<JdbcDataSource, XATransactionConnection> connectionMap = new ConcurrentHashMap<>();
  private boolean autocommit = false;
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
    try {
      userTransaction.begin();
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  public AbsractConnection getConnection(JdbcDataSource jdbcDataSource) {
    beforeDoAction();
    XATransactionConnection connection = connectionMap.compute(jdbcDataSource,
        new BiFunction<JdbcDataSource, XATransactionConnection, XATransactionConnection>() {
          @Override
          public XATransactionConnection apply(JdbcDataSource dataSource,
              XATransactionConnection absractConnection) {
            if (absractConnection != null) {
              if (absractConnection.isClosed()) {
                if (!isInTransaction()) {
                  return transactionProcessUnit
                      .getXATransactionConnection(jdbcDataSource, transactionIsolation);
                }
                throw new MycatException("11111111111111");
              }
              return absractConnection;
            } else {
              return transactionProcessUnit
                  .getXATransactionConnection(jdbcDataSource, transactionIsolation);
            }
          }
        });

    if (connection.isClosed()) {
      throw new MycatException("11111111111111");
    }
    return connection;
  }

  @Override
  public void commit() {
    try {
      userTransaction.commit();
    } catch (Exception e) {
      throw new MycatException(e);
    }
    afterDoAction();
  }

  @Override
  public void rollback() {
    try {
      userTransaction.begin();
    } catch (Exception e) {
      throw new MycatException(e);
    }
    afterDoAction();
  }

  @Override
  public boolean isInTransaction() {
    try {
      return Status.STATUS_NO_TRANSACTION != userTransaction.getStatus();
    } catch (SystemException e) {
      throw new MycatException(e);
    }
  }

  @Override
  public void beforeDoAction() {
    try {
      if (!this.autocommit && Status.STATUS_NO_TRANSACTION == userTransaction.getStatus()) {
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
    // connectionMap.values().forEach(AbsractConnection::close);
    // connectionMap.clear();
  }
}