package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.compute.RowBaseIterator;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author jamie12221 date 2019-05-10 14:51
 **/
public class JdbcSession {

  private final Connection connection;
  private final JdbcDataSource key;

  public JdbcSession(Connection connection, JdbcDataSource key) {

    this.connection = connection;
    this.key = key;
  }

  public JdbcDataSource getDatasource() {
    return key;
  }

  public void sync(String schema, MySQLIsolation isolation,
      MySQLAutoCommit autoCommit, String charset) throws SQLException {
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
    connection.setSchema(schema);
    connection.setAutoCommit(autoCommit == MySQLAutoCommit.ON);
    connection.setClientInfo("characterEncoding", charset);

  }

  public RowBaseIterator query(String s) throws MycatException {
    try {
      Statement statement = connection.createStatement();
      return new JdbcRowBaseIteratorImpl(statement.executeQuery(s));
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }
}
