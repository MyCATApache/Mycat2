/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc.datasource;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
/**
 * @author Junwen Chen
 **/
public class JTATransactionSessionImpl implements TransactionSession {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(JTATransactionSessionImpl.class);
  private final UserTransaction userTransaction;
  private final GThread gThread;
  private final Map<JdbcDataSource, DsConnection> connectionMap = new HashMap<>();
  private volatile boolean autocommit = true;
  private volatile boolean inTranscation = false;
  private volatile int transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;

  public JTATransactionSessionImpl(UserTransaction userTransaction,
      GThread gThread) {
    this.userTransaction = userTransaction;
    this.gThread = gThread;
  }

  @Override
  public void setTransactionIsolation(int transactionIsolation) {
    this.transactionIsolation = transactionIsolation;
    this.connectionMap.values().forEach(c -> c.setTransactionIsolation(transactionIsolation));
  }

  @Override
  public void begin() {
    inTranscation = true;
    connectionMap.values().forEach(c -> c.close());
    connectionMap.clear();
    try {
      LOGGER.debug("{} begin", userTransaction);
      userTransaction.begin();
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  public DsConnection getConnection(JdbcDataSource jdbcDataSource) {
    beforeDoAction();
    return connectionMap.compute(jdbcDataSource,
        new BiFunction<JdbcDataSource, DsConnection, DsConnection>() {
          @Override
          public DsConnection apply(JdbcDataSource dataSource,
              DsConnection absractConnection) {
            if (absractConnection != null) {
              return absractConnection;
            } else {
              return gThread
                  .getConnection(jdbcDataSource, transactionIsolation);
            }
          }
        });
  }

  @Override
  public void commit() {
    inTranscation = false;
    try {
      userTransaction.commit();
    } catch (Exception e) {
      LOGGER.error("", e);
      throw new MycatException(e);
    }
    afterDoAction();
  }

  @Override
  public void rollback() {
    inTranscation = false;
    try {
      userTransaction.rollback();
    } catch (Exception e) {
      throw new MycatException(e);
    }
    afterDoAction();
  }

  @Override
  public boolean isInTransaction() {
    try {
//      int status = userTransaction.getStatus();
//      LOGGER.debug("()()()()()()()(({}", status);
//      switch (status) {
//        case Status.STATUS_NO_TRANSACTION:
//        case Status.STATUS_UNKNOWN:
//        case Status.STATUS_MARKED_ROLLBACK:
//        case Status.STATUS_COMMITTED:
//        case Status.STATUS_ROLLEDBACK:
//          return inTranscation;
//        default:
//        case Status.STATUS_ACTIVE:
//          return inTranscation = true;
//      }
      return inTranscation;
    } catch (Exception e) {
      throw new MycatException(e);
    }
  }

  @Override
  public void beforeDoAction() {
    try {
      if (!this.autocommit && !isInTransaction()) {
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
    connectionMap.values().forEach(DsConnection::close);
    connectionMap.clear();
  }
}