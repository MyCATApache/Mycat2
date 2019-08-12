package io.mycat.datasource.jdbc.datasource;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DefaultConnection implements DsConnection {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(DefaultConnection.class);
  final Connection connection;
  private final JdbcDataSource jdbcDataSource;
  private volatile boolean isClosed = false;
  protected final ConnectionManager connectionManager;

  public DefaultConnection(Connection connection, JdbcDataSource dataSource,
      boolean autocommit,
      int transactionIsolation, ConnectionManager connectionManager) {
    this(connection, dataSource, connectionManager);
    try {
      if (!autocommit) {
        connection.setAutoCommit(false);
      }
      if (Connection.TRANSACTION_REPEATABLE_READ != transactionIsolation) {
        connection.setTransactionIsolation(transactionIsolation);
      }
    } catch (SQLException e) {
      LOGGER.error("", e);
      throw new MycatException(e);
    }
  }

  public DefaultConnection(Connection connection, JdbcDataSource jdbcDataSource,
      ConnectionManager connectionManager) {
    this.connection = connection;
    this.jdbcDataSource = jdbcDataSource;
    this.connectionManager = connectionManager;
  }


  public MycatUpdateResponse executeUpdate(String sql, boolean needGeneratedKeys) {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql,
          needGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
      int lastInsertId = 0;
      if (needGeneratedKeys) {
        ResultSet generatedKeys = statement.getGeneratedKeys();
        lastInsertId = (int) (generatedKeys.next() ? generatedKeys.getLong(0) : 0L);
      }
      return new MycatUpdateResponseImpl(statement.getUpdateCount(), lastInsertId,
          MySQLServerStatusFlags.AUTO_COMMIT);
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }


  public JdbcRowBaseIteratorImpl executeQuery(String sql) {
    try {
      Statement statement = connection.createStatement();
      return new JdbcRowBaseIteratorImpl(statement, statement.executeQuery(sql));
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }


  public void onExceptionClose() {
    close();
  }


  public void close() {
    try {
      if (!isClosed) {
        isClosed = true;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("close {}", connection);
        }
        connectionManager.closeConnection(jdbcDataSource, connection);
      }
    } catch (Exception e) {
      LOGGER.error("", e);
    }
  }

  public void setTransactionIsolation(int transactionIsolation) {
    try {
      if (connection.getTransactionIsolation() != transactionIsolation) {
        this.connection.setTransactionIsolation(transactionIsolation);
      }
    } catch (SQLException e) {
      throw new MycatException(e);
    }
  }

  public JdbcDataSource getDataSource() {
    return jdbcDataSource;
  }

  public boolean isClosed() {
    try {
      return isClosed || connection.isClosed();
    } catch (SQLException e) {
      LOGGER.error("", e);
      return true;
    }
  }
}