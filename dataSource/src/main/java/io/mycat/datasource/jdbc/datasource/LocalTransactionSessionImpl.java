package io.mycat.datasource.jdbc.datasource;

import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class LocalTransactionSessionImpl implements TransactionSession {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(LocalTransactionSessionImpl.class);

  private final GThread gthread;
  private final Map<JdbcDataSource, DsConnection> connectionMap = new HashMap<>();
  private boolean autocommit = false;
  private boolean isTrancation = false;
  private int transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;

  public LocalTransactionSessionImpl(GThread gthread) {
    this.gthread = gthread;
  }

  public DsConnection getConnection(JdbcDataSource jdbcDataSource) {
    beforeDoAction();
    return connectionMap.compute(jdbcDataSource,
        new BiFunction<JdbcDataSource, DsConnection, DsConnection>() {
          @Override
          public DsConnection apply(JdbcDataSource dataSource,
              DsConnection absractConnection) {
            if (absractConnection == null) {
              if (isTrancation) {
                return gthread.getConnection(dataSource, false, transactionIsolation);
              } else {
                return gthread
                    .getConnection(dataSource, transactionIsolation);
              }
            } else {
              return absractConnection;
            }
          }
        });
  }

  @Override
  public void setTransactionIsolation(int transactionIsolation) {
    this.transactionIsolation = transactionIsolation;
  }

  @Override
  public void begin() {
    beforeDoAction();
    connectionMap.values().forEach(c -> c.close());
    connectionMap.clear();
    isTrancation = true;
  }

  @Override
  public void commit() {
    isTrancation = false;
    for (DsConnection value : connectionMap.values()) {
      try {
        ((DefaultConnection) value).connection.commit();
      } catch (SQLException e) {
        LOGGER.error("", e);
      }
    }
    afterDoAction();
  }

  @Override
  public void rollback() {
    isTrancation = false;
    for (DsConnection value : connectionMap.values()) {
      try {
        ((DefaultConnection) value).connection.rollback();
      } catch (SQLException e) {
        LOGGER.error("", e);
      }
    }
    afterDoAction();
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