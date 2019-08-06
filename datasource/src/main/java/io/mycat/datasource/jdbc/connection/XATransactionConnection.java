package io.mycat.datasource.jdbc.connection;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;

public class XATransactionConnection extends AbsractConnection {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(XATransactionConnection.class);

  public XATransactionConnection(Connection connection, JdbcDataSource dataSource,
      int transactionIsolation) {
    super(connection, dataSource);
    try {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(transactionIsolation);
    } catch (SQLException e) {
      LOGGER.error("", e);
      throw new MycatException(e);
    }
  }

}