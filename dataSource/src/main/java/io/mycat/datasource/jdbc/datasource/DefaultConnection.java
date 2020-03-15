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
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import lombok.SneakyThrows;

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

    @SneakyThrows
    public DefaultConnection(Connection connection, JdbcDataSource dataSource,
                             Boolean autocommit,
                             int transactionIsolation, boolean readOnly, ConnectionManager connectionManager) {
        this.connection = connection;
        this.jdbcDataSource = dataSource;
        this.connectionManager = connectionManager;
        if (autocommit != null) {
            connection.setAutoCommit(autocommit);
        }
        connection.setReadOnly(readOnly);
        connection.setTransactionIsolation(transactionIsolation);
    }


    public UpdateRowIteratorResponse executeUpdate(String sql, boolean needGeneratedKeys, int serverStatus) {
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
            return new UpdateRowIteratorResponse(statement.getUpdateCount(), lastInsertId, serverStatus);
        } catch (Exception e) {
            throw new MycatException(e);
        }
    }


    public RowBaseIterator executeQuery(String sql) {
        try {
            Statement statement = connection.createStatement();
            return new JdbcRowBaseIterator(statement, statement.executeQuery(sql));
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