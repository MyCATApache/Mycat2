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
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;

import java.sql.*;

/**
 * @author Junwen Chen
 **/
public class DefaultConnection implements AutoCloseable {

    private static final MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(DefaultConnection.class);
    final Connection connection;
    private final JdbcDataSource jdbcDataSource;
    private volatile boolean isClosed = false;
    protected final ConnectionManager connectionManager;

    public DefaultConnection(Connection connection, JdbcDataSource dataSource,
                             boolean autocommit,
                             int transactionIsolation, boolean readOnly, ConnectionManager connectionManager) {
        this.connection = connection;
        this.jdbcDataSource = dataSource;
        this.connectionManager = connectionManager;
        try {
            if (!autocommit) {
                connection.setAutoCommit(false);
            }
            connection.setReadOnly(readOnly);
            connection.setTransactionIsolation(transactionIsolation);
        } catch (SQLException e) {
            LOGGER.error("", e);
            throw new MycatException(e);
        }
    }


    public MycatUpdateResponse executeUpdate(String sql, boolean needGeneratedKeys) {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql,
                    needGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
            long lastInsertId = 0;
            if (needGeneratedKeys) {
                ResultSet generatedKeys = statement.getGeneratedKeys();
                ResultSetMetaData metaData = generatedKeys.getMetaData();
                if (metaData.getColumnCount() == 1) {
                    lastInsertId = (generatedKeys.next() ? generatedKeys.getLong(1) : 0L);
                }
            }
            int serverStatus = TransactionSessionUtil.currentTransactionSession().getServerStatus();
            return new MycatUpdateResponseImpl(statement.getUpdateCount(), lastInsertId, serverStatus);
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
                connectionManager.closeConnection(this);
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

    public void setReadyOnly(boolean readyOnly) {
        try {
            if (connection.isReadOnly() != readyOnly) {
                this.connection.setReadOnly(readyOnly);
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

    public Connection getRawConnection() {
        return connection;
    }

}