package io.mycat.datasource.jdbc.session;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.connection.AbsractConnection;
import io.mycat.datasource.jdbc.connection.TransactionConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.manager.TransactionProcessUnit;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class LocalTransactionSessionImpl implements TransactionSession {

  private final TransactionProcessUnit transactionProcessUnit;
  private final Map<JdbcDataSource, AbsractConnection> connectionMap = new HashMap<>();
  private boolean autocommit = false;
  private boolean isTrancation = false;
  private int transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;

  public LocalTransactionSessionImpl(
      TransactionProcessUnit transactionProcessUnit) {
    this.transactionProcessUnit = transactionProcessUnit;
  }

  public AbsractConnection getConnection(JdbcDataSource jdbcDataSource) {
    beforeDoAction();
    AbsractConnection connection = connectionMap.compute(jdbcDataSource,
        new BiFunction<JdbcDataSource, AbsractConnection, AbsractConnection>() {
          @Override
          public AbsractConnection apply(JdbcDataSource dataSource,
              AbsractConnection absractConnection) {
            if (absractConnection == null) {
              if (isTrancation) {
                return transactionProcessUnit.getAutocommitConnection(dataSource);
              } else {
                return transactionProcessUnit
                    .getLocalTransactionConnection(dataSource, transactionIsolation);
              }
            } else {
              return absractConnection;
            }
          }
        });
    return connection;
  }

  @Override
  public void setTransactionIsolation(int transactionIsolation) {
    this.transactionIsolation = transactionIsolation;
  }

  @Override
  public void begin() {
    beforeDoAction();
    isTrancation = true;
  }

  @Override
  public void commit() {
    for (AbsractConnection value : connectionMap.values()) {
      if (value instanceof TransactionConnection) {
        ((TransactionConnection) value).commit();
      } else {
        throw new MycatException("unknown state");
      }
    }
    connectionMap.clear();
  }

  @Override
  public void rollback() {
    for (AbsractConnection value : connectionMap.values()) {
      if (value instanceof TransactionConnection) {
        ((TransactionConnection) value).rollback();
      } else {
        throw new MycatException("unknown state");
      }
    }
    connectionMap.clear();
  }

  @Override
  public boolean isInTransaction() {
    return isTrancation;
  }

  @Override
  public void beforeDoAction() {
    if (autocommit) {
      connectionMap.values().forEach(c -> c.close());
      connectionMap.clear();
    } else {
      isTrancation = true;
    }
  }

  @Override
  public void afterDoAction() {
    if (!isTrancation) {
      connectionMap.values().forEach(c -> c.close());
      connectionMap.clear();
    }
  }

  @Override
  public void setAutocommit(boolean autocommit) {
    this.autocommit = autocommit;
  }
}