package io.mycat.datasource.jdbc.connection;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;

public class LocalTransactionConnection extends AbsractConnection implements TransactionConnection {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(LocalTransactionConnection.class);

  public LocalTransactionConnection(Connection connection, JdbcDataSource dataSource,
      int transactionIsolation, ConnectionManager connectionManager) {
    super(connection, dataSource, connectionManager);
    try {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(transactionIsolation);
    } catch (SQLException e) {
      LOGGER.error("", e);
      throw new MycatException(e);
    }
  }

  @Override
  public void commit() {
    try {
      connection.commit();
    } catch (SQLException e) {
      LOGGER.error("", e);
    } finally {
      close();
    }
  }

  @Override
  public void rollback() {
    try {
      connection.commit();
    } catch (SQLException e) {
      LOGGER.error("", e);
    } finally {
      close();
    }
  }

  @Override
  public boolean isInTransaction() {
    try {
      return !connection.isClosed();
    } catch (SQLException e) {
      LOGGER.error("", e);
      onExceptionClose();
      return false;
    }
  }

  @Override
  public void onExceptionClose() {
    close();
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error("", e);
    }
  }
}