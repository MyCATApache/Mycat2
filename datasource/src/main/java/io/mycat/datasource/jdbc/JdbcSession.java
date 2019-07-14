package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author jamie12221 date 2019-05-10 14:51
 **/
public class JdbcSession {

  protected final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcSession.class);
  protected final int sessionId;
  protected final JdbcDataSource key;
  protected volatile Connection connection;

  public JdbcSession(int sessionId, JdbcDataSource key) {
    this.sessionId = sessionId;
    this.key = key;
  }

  public void wrap(Connection connection) {
    this.connection = connection;
  }

  public JdbcDataSource getDatasource() {
    return key;
  }

  public void sync(MySQLIsolation isolation,
      MySQLAutoCommit autoCommit) {
    try {
      switch (isolation) {
        case READ_UNCOMMITTED:
          connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
          break;
        case READ_COMMITTED:
          connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
          break;
        case REPEATED_READ:
          connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
          break;
        case SERIALIZABLE:
          connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
          break;
      }
      connection.setAutoCommit(autoCommit == MySQLAutoCommit.ON);
//      connection.setClientInfo("characterEncoding", charset);
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }


  public void close(boolean normal, String reason) {
    LOGGER.debug("jdbc sessionId:{} normal:{} reason:{}", sessionId, normal, reason);
    try {
      connection.close();
    } catch (Exception e) {
      LOGGER.debug("", e);
    }
  }

  public int sessionId() {
    return sessionId;
  }

  public boolean isIdle() {
    return connection == null;
  }


  public MycatUpdateResponse executeUpdate(String sql, boolean needGeneratedKeys) {
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql,
          needGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
      long lastInsertId = 0L;
      if (needGeneratedKeys) {
        ResultSet generatedKeys = statement.getGeneratedKeys();
        lastInsertId = generatedKeys.next() ? generatedKeys.getLong(0) : 0L;
      }
      return new MycatUpdateResponseImpl(statement.getUpdateCount(), lastInsertId);
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
}
