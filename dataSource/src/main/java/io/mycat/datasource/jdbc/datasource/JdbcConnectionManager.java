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
import io.mycat.config.DatasourceRootConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class JdbcConnectionManager implements ConnectionManager {

    private final static MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JdbcConnectionManager.class);
    private final ConcurrentHashMap<String, JdbcDataSource> dataSourceMap = new ConcurrentHashMap<>();
    private final DatasourceProvider datasourceProvider;


    public JdbcConnectionManager(DatasourceProvider provider) {
        this.datasourceProvider = Objects.requireNonNull(provider);
    }

    @Override
    public void addDatasource(DatasourceRootConfig.DatasourceConfig key) {
        dataSourceMap.computeIfAbsent(key.getName(), dataSource1 -> datasourceProvider
                .createDataSource(key));
    }

    @Override
    public void removeDatasource(String jdbcDataSourceName) {
        dataSourceMap.remove(jdbcDataSourceName);
    }

    public DefaultConnection getConnection(String name) {
        return getConnection(name, true, Connection.TRANSACTION_REPEATABLE_READ,false);
    }

    public DefaultConnection getConnection(String name, boolean autocommit,
                                           int transactionIsolation, boolean readOnly) {
        JdbcDataSource key = dataSourceMap.get(name);
        if (key.counter.updateAndGet(operand -> {
            if (operand < key.getMaxCon()) {
                return ++operand;
            }
            return operand;
        }) < key.getMaxCon()) {
            try {
                return new DefaultConnection(key.dataSource.getConnection(), key, autocommit, transactionIsolation,readOnly, this);
            } catch (SQLException e) {
                key.counter.decrementAndGet();
                throw new MycatException(e);
            }
        } else {
            throw new MycatException("max limit");
        }
    }

    @Override
    public void closeConnection(DefaultConnection connection) {
        connection.getDataSource().counter.updateAndGet(operand -> {
            if (operand == 0) {
                return 0;
            }
            return --operand;
        });
        try {
            connection.connection.close();
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }

    public Map<String, JdbcDataSource> getDatasourceInfo() {
        return Collections.unmodifiableMap(dataSourceMap);
    }


}
