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
import io.mycat.replica.ReplicaSelectorRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class JdbcConnectionManager implements ConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnectionManager.class);
    private final ConcurrentHashMap<String, JdbcDataSource> dataSourceMap = new ConcurrentHashMap<>();
    private final DatasourceProvider datasourceProvider;


    public JdbcConnectionManager(DatasourceProvider provider) {
        this.datasourceProvider = Objects.requireNonNull(provider);
    }

    @Override
    public void addDatasource(DatasourceRootConfig.DatasourceConfig key) {
        dataSourceMap.computeIfAbsent(key.getName(), dataSource1 -> {
            JdbcDataSource dataSource = datasourceProvider.createDataSource(key);
            ReplicaSelectorRuntime.INSTANCE.registerDatasource(dataSource1, () -> dataSource.counter.get());
            return dataSource;
        });
    }

    @Override
    public void removeDatasource(String jdbcDataSourceName) {
        JdbcDataSource remove = dataSourceMap.remove(jdbcDataSourceName);
        Optional.ofNullable(remove).map(i -> i.getDataSource()).ifPresent(i -> {
            try {
                Class<? extends DataSource> aClass = i.getClass();
                Method[] methods = aClass.getMethods();
                ArrayList<Method> methodList = new ArrayList<>();
                for (Method method : methods) {
                    if ("close".equals(method.getName())) {
                        if (Void.TYPE.equals(method.getReturnType())) {
                            methodList.add(method);
                        }
                    }
                }
                methodList.sort(Comparator.comparingInt(Method::getParameterCount));
                if (!methodList.isEmpty()) {
                    methodList.get(0).invoke(i);
                }
            } catch (Throwable e) {
                LOGGER.warn("试图关闭数据源失败:{} ,{}", jdbcDataSourceName, e);
            }
        });
    }

    public DefaultConnection getConnection(String name) {
        return getConnection(name, true, Connection.TRANSACTION_REPEATABLE_READ, false);
    }

    public DefaultConnection getConnection(String name, Boolean autocommit,
                                           int transactionIsolation, boolean readOnly) {
        JdbcDataSource key = Optional.ofNullable(dataSourceMap.get(name))
                .orElseGet(()->{
                  return dataSourceMap.get( ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(name,true,null));
                });
        if (key.counter.updateAndGet(operand -> {
            if (operand < key.getMaxCon()) {
                return ++operand;
            }
            return operand;
        }) < key.getMaxCon()) {
            DefaultConnection defaultConnection;
            try {
                DatasourceRootConfig.DatasourceConfig config = key.getConfig();
                Connection connection = key.getDataSource().getConnection();
                defaultConnection = new DefaultConnection(connection, key, autocommit, transactionIsolation, readOnly, this);
                try {
                    return defaultConnection;
                } finally {
                    LOGGER.info("获取连接:{} {}",name,defaultConnection);
                    if (config.isInitSqlsGetConnection()) {
                        if (config.getInitSqls() != null && !config.getInitSqls().isEmpty()) {
                            try (Statement statement = connection.createStatement()) {
                                for (String initSql : config.getInitSqls()) {
                                    statement.execute(initSql);
                                }
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                LOGGER.debug("", e);
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
        LOGGER.info("关闭连接:{}",connection);
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
