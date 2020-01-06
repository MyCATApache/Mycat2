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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class JTATransactionSessionImpl implements TransactionSession {

    private static final MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JTATransactionSessionImpl.class);
    private final UserTransaction userTransaction;
    private final GThread gThread;
    private final Map<String, DefaultConnection> connectionMap = new HashMap<>();
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
        for (DefaultConnection c : this.connectionMap.values()) {
            c.setTransactionIsolation(transactionIsolation);
        }
    }

    @Override
    public void begin() {
        inTranscation = true;
        for (DefaultConnection c : connectionMap.values()) {
            c.close();
        }
        connectionMap.clear();
        try {
            LOGGER.debug("{} begin", userTransaction);
            userTransaction.begin();
        } catch (Exception e) {
            throw new MycatException(e);
        }
    }

    public DefaultConnection getConnection(String jdbcDataSource) {
        Objects.requireNonNull(jdbcDataSource);
        beforeDoAction();
        return connectionMap.compute(jdbcDataSource,
                (dataSource, absractConnection) -> {
                    if (absractConnection != null) {
                        return absractConnection;
                    } else {
                        return JdbcRuntime.INSTANCE
                                .getConnection(jdbcDataSource, autocommit, transactionIsolation);
                    }
                });
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
        connectionMap.values().forEach(DefaultConnection::close);
        connectionMap.clear();
    }
}