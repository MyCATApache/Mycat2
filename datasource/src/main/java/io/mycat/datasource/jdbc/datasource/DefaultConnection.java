/**
 * Copyright (C) <2021>  <chen junwen>
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

import io.mycat.ConnectionManager;
import io.mycat.MycatConnection;
import io.mycat.MycatException;
import io.mycat.api.collector.*;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.Observable;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Junwen Chen
 **/
public class DefaultConnection implements MycatConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConnection.class);
    final Connection connection;
    private final JdbcDataSource jdbcDataSource;
    protected final ConnectionManager connectionManager;
    private boolean closed = false;

    @SneakyThrows
    public DefaultConnection(Connection connection, JdbcDataSource dataSource,
                             Boolean autocommit,
                             int transactionIsolation,ConnectionManager connectionManager) {
        this.connection = connection;
        this.jdbcDataSource = dataSource;
        this.connectionManager = connectionManager;
        if (autocommit != null) {
            connection.setAutoCommit(autocommit);
        }
//        connection.setReadOnly(readOnly);
        connection.setTransactionIsolation(transactionIsolation);
    }


    public long[] executeUpdate(String sql, boolean needGeneratedKeys) {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql,
                    needGeneratedKeys ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
            if (needGeneratedKeys) {
                ResultSet generatedKeys = statement.getGeneratedKeys();
                if (generatedKeys != null && generatedKeys.next()) {
                    BigDecimal decimal = generatedKeys.getBigDecimal(1);
                    return new long[]{statement.getUpdateCount(), decimal.longValue()};
                }
            }
            return new long[]{statement.getUpdateCount(), 0};
        } catch (Exception e) {
            LOGGER.error("",e);
            throw new MycatException(e);
        }
    }


    public RowBaseIterator executeQuery(String sql) {
        try {
            Statement statement = connection.createStatement();
            statement.setFetchSize(1);
            ResultSet resultSet = statement.executeQuery(sql);
            return new JdbcRowBaseIterator(null, this, statement, resultSet, new RowIteratorCloseCallback() {

                @Override
                public void onClose(long rowCount) {

                }
            }, sql);
        } catch (Exception e) {
            throw new MycatException(e);
        }
    }


    public void onExceptionClose() {
        close();
    }


    public void close() {
        try {
            if (!isClosed()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("close {}", connection);
                }
                closed = true;
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
        //            if (connection.isReadOnly() != readyOnly) {
//                this.connection.setReadOnly(readyOnly);
//            }
    }

    public JdbcDataSource getDataSource() {
        return jdbcDataSource;
    }

    public boolean isClosed() {
        return closed;
    }

    public Connection getRawConnection() {
        return connection;
    }

    public Observable<MysqlPayloadObject> executeQuery(MycatRowMetaData mycatRowMetaData, String sql) {
        return (Observable.create(emitter -> {
            try (Statement statement = connection.createStatement();
                 RowBaseIterator rowBaseIterator = new JdbcRowBaseIterator(mycatRowMetaData, DefaultConnection.this, statement, statement.executeQuery(sql), null, sql);) {
                MycatRowMetaData metaData = rowBaseIterator.getMetaData();
                emitter.onNext(new MySQLColumnDef(metaData));
                while (rowBaseIterator.next()) {
                    emitter.onNext(new MysqlObjectArrayRow(rowBaseIterator.getObjects()));
                }
                emitter.onComplete();
            } catch (Throwable throwable) {
                emitter.onError(throwable);
            }
        }));
    }

    @Override
    @SneakyThrows
    public <T> T unwrap(Class<T> iface) {
        if (Connection.class == iface) {
            return (T) connection;
        }
        return connection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return unwrap(iface) != null;
    }

    public void deleteTable(String schemaName, String tableName) {
        String dropTemplate = "drop table `%s`.`%s`";
        executeUpdate(String.format(dropTemplate, schemaName, tableName), false);
    }

    public void createTable(String rewriteCreateTableSql) {
        executeUpdate(rewriteCreateTableSql, false);
    }

    public void createDatabase(String schema) {
        executeUpdate("CREATE DATABASE IF NOT EXISTS " + schema+ " DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ", false);
    }
}