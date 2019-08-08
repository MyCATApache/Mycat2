package io.mycat.datasource.jdbc.connection;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;

public class DefaultTransactionConnection extends AbsractConnection {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(DefaultTransactionConnection.class);

  public DefaultTransactionConnection(Connection connection, JdbcDataSource dataSource,
      boolean autocommit,
      int transactionIsolation, ConnectionManager connectionManager) {
    super(connection, dataSource, connectionManager);
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

}