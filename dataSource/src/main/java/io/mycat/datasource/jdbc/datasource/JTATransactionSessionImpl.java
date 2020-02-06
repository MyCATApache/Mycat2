/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc.datasource;

import io.mycat.MycatException;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;

import javax.transaction.UserTransaction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Junwen Chen
 **/
public class JTATransactionSessionImpl implements TransactionSession {

    private static final MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JTATransactionSessionImpl.class);
    private final UserTransaction userTransaction;
    private final GThread gThread;
    private final Map<String, DefaultConnection> updateConnectionMap = new HashMap<>();
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
        for (DefaultConnection c : this.updateConnectionMap.values()) {
            try {
                if (c.getRawConnection().getTransactionIsolation() != transactionIsolation) {
                    c.setTransactionIsolation(transactionIsolation);
                }
            } catch (SQLException e) {
                throw new MycatException(e);
            }

        }
    }

    @Override
    public void begin() {
        if (!isInTransaction()) {
            inTranscation = true;
            for (DefaultConnection c : updateConnectionMap.values()) {
                c.close();
            }
            updateConnectionMap.clear();
            try {
                LOGGER.debug("{} begin", userTransaction);
                userTransaction.begin();
            } catch (Exception e) {
                throw new MycatException(e);
            }
        }
    }

    public DefaultConnection getConnection(String jdbcDataSource) {
        Objects.requireNonNull(jdbcDataSource);
        beforeDoAction();
        return updateConnectionMap.compute(jdbcDataSource,
                (dataSource, absractConnection) -> {
                    if (absractConnection != null && !absractConnection.isClosed()) {
                        return absractConnection;
                    } else {
                        return JdbcRuntime.INSTANCE
                                .getConnection(jdbcDataSource, autocommit, transactionIsolation);
                    }
                });
    }

    @Override
    public DefaultConnection getDisposableConnection(String jdbcDataSource) {
        return JdbcRuntime.INSTANCE
                .getConnection(jdbcDataSource, autocommit, transactionIsolation);
    }

    @Override
    public DisposQueryConnection getDisposableConnection(List<String> jdbcDataSourceList) {
        Map<String,LinkedList<DefaultConnection>> map = new HashMap<>();
        for (String s : jdbcDataSourceList) {
            List<DefaultConnection> defaultConnections = map.computeIfAbsent(s, s1 -> new LinkedList<>());
            defaultConnections.add(JdbcRuntime.INSTANCE.getConnection(s, autocommit, transactionIsolation));
        }
        return new DisposQueryConnection(map);
    }


    @Override
    public void reset() {
        if (isInTransaction()) {
            rollback();
        }
        afterDoAction();
    }


    @Override
    public void commit() {
        try {//真正开启事务才提交
            if (isInTransaction()) {
                userTransaction.commit();
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new MycatException(e);
        } finally {
            inTranscation = false;
        }
        afterDoAction();
    }

    @Override
    public void rollback() {
        inTranscation = false;
        try {
            userTransaction.rollback();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        afterDoAction();
    }

    @Override
    public boolean isInTransaction() {
        return inTranscation;
    }

    @Override
    public void beforeDoAction() {
        if (!this.autocommit && !isInTransaction()) {
            begin();
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
        updateConnectionMap.values().forEach(DefaultConnection::close);
        updateConnectionMap.clear();
    }
}