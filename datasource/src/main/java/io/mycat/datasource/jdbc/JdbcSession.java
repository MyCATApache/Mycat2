package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author jamie12221 date 2019-05-10 14:51
 **/
public class JdbcSession implements ClearableSession {

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
      int transactionIsolation = connection.getTransactionIsolation();
      int jdbcValue = isolation.getJdbcValue();
      if (transactionIsolation != jdbcValue) {
        connection.setTransactionIsolation(jdbcValue);
      }
      connection.setAutoCommit(autoCommit == MySQLAutoCommit.ON);
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

  public JdbcRowBaseIteratorImpl executeQuery(ClearableSession session,
      String sql) {
    try {
      Statement statement = connection.createStatement();
      return new JdbcRowBaseIteratorImpl(session, statement, statement.executeQuery(sql));
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  public void commit() {
    try {
      connection.commit();
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  public void rollback() {
    try {
      connection.rollback();
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  public void setTransactionIsolation(MySQLIsolation isolation) {
    try {
      connection.setTransactionIsolation(isolation.getJdbcValue());
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  public void setAutomcommit(boolean on) {
    try {
      connection.setAutoCommit(on);
    } catch (SQLException e) {
      throw new MycatException(e);
    }
  }

  @Override
  public void clear() {
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error("", e);
    }
  }
}
