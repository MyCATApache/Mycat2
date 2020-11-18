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


import io.mycat.ConnectionManager;
import io.mycat.MycatException;
import io.mycat.MycatWorkerProcessor;
import io.mycat.ScheduleUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.datasourceprovider.DruidDatasourceProvider;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author jamie12221 date 2019-05-10 14:46 该类型需要并发处理
 **/
public class JdbcConnectionManager implements ConnectionManager<DefaultConnection> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnectionManager.class);
    private final ConcurrentHashMap<String, JdbcDataSource> dataSourceMap = new ConcurrentHashMap<>();
    private final DatasourceProvider datasourceProvider;
    private final MycatWorkerProcessor workerProcessor;
    private final ReplicaSelectorRuntime replicaSelector;

    public JdbcConnectionManager(String customerDatasourceProvider,
                                 Map<String,DatasourceConfig> datasources,
                                 Map<String,ClusterConfig> clusterConfigs,
                                 MycatWorkerProcessor workerProcessor,
                                 ReplicaSelectorRuntime replicaSelector) {
        this(datasources, clusterConfigs, createDatasourceProvider(customerDatasourceProvider), workerProcessor, replicaSelector);
    }

    private static DatasourceProvider createDatasourceProvider(String customerDatasourceProvider) {
        String defaultDatasourceProvider = Optional.ofNullable(customerDatasourceProvider).orElse(DruidDatasourceProvider.class.getName());
        try {
            return (DatasourceProvider) Class.forName(defaultDatasourceProvider)
                    .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new MycatException("can not load datasourceProvider:{}", customerDatasourceProvider);
        }
    }

    public JdbcConnectionManager(Map<String,DatasourceConfig> datasources,
                                 Map<String,ClusterConfig> clusterConfigs,
                                 DatasourceProvider provider,
                                 MycatWorkerProcessor workerProcessor,
                                 ReplicaSelectorRuntime replicaSelector) {
        this.datasourceProvider = Objects.requireNonNull(provider);
        this.workerProcessor = workerProcessor;
        this.replicaSelector = replicaSelector;

        for (DatasourceConfig datasource : datasources.values()) {
            if (datasource.computeType().isJdbc()) {
                addDatasource(datasource);
            }
        }

        for (ClusterConfig replica : clusterConfigs.values()) {
            String replicaName = replica.getName();
            for (String datasource : replica.allDatasources()) {
                putHeartFlow(replicaName, datasource);
            }
        }

        //移除不必要的配置
        //新配置中的数据源名字
        Set<String> datasourceNames = datasources.keySet();
        Map<String, JdbcDataSource> datasourceInfo = this.getDatasourceInfo();
        new HashSet<>(datasourceInfo.keySet()).stream().filter(name -> !datasourceNames.contains(name)).forEach(name -> removeDatasource(name));
    }

    @Override
    public void addDatasource(DatasourceConfig key) {
        dataSourceMap.computeIfAbsent(key.getName(), dataSource1 -> {
            JdbcDataSource dataSource = datasourceProvider.createDataSource(key);
            replicaSelector.registerDatasource(dataSource1, () -> dataSource.counter.get());
            return dataSource;
        });
    }

    @Override
    public void removeDatasource(String jdbcDataSourceName) {
        JdbcDataSource remove = dataSourceMap.remove(jdbcDataSourceName);
        if (remove!=null){
            remove.close();
        }
    }

    public DefaultConnection getConnection(String name) {
        return getConnection(name, true, Connection.TRANSACTION_REPEATABLE_READ, false);
    }

    public DefaultConnection getConnection(String name, Boolean autocommit,
                                           int transactionIsolation, boolean readOnly) {
        JdbcDataSource key = Objects.requireNonNull(Optional.ofNullable(dataSourceMap.get(name))
                .orElseGet(() -> {
                    return dataSourceMap.get(replicaSelector.getDatasourceNameByReplicaName(name, true, null));
                }),()->"unknown target:"+name);
        if (key.counter.updateAndGet(operand -> {
            if (operand < key.getMaxCon()) {
                return ++operand;
            }
            return operand;
        }) < key.getMaxCon()) {
            DefaultConnection defaultConnection;
            try {
                DatasourceConfig config = key.getConfig();
                Connection connection = key.getDataSource().getConnection();
                defaultConnection = new DefaultConnection(connection, key, autocommit, transactionIsolation, readOnly, this);
                try {
                    return defaultConnection;
                } finally {
                    LOGGER.debug("获取连接:{} {}", name, defaultConnection);
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
        LOGGER.debug("关闭连接:{}", connection);
        try {
            connection.connection.close();
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }

    public Map<String, JdbcDataSource> getDatasourceInfo() {
        return Collections.unmodifiableMap(dataSourceMap);
    }

    private void putHeartFlow(String replicaName, String datasource) {
        replicaSelector.putHeartFlow(replicaName, datasource, new Consumer<HeartBeatStrategy>() {
            @Override
            public void accept(HeartBeatStrategy heartBeatStrategy) {
                workerProcessor.getMycatWorker().submit(() -> {
                    try {
                        heartbeat(heartBeatStrategy);
                    } catch (Exception e) {
                        heartBeatStrategy.onException(e);
                    }
                });
            }

            private void heartbeat(HeartBeatStrategy heartBeatStrategy) {
                DefaultConnection connection = null;
                try {
                    connection = getConnection(datasource);
                    List<Map<String, Object>> resultList;
                    try (RowBaseIterator iterator = connection
                            .executeQuery(heartBeatStrategy.getSql())) {
                        resultList = iterator.getResultSetMap();
                    }
                    LOGGER.debug("jdbc heartbeat {}", Objects.toString(resultList));
                    heartBeatStrategy.process(resultList);
                } catch (Exception e) {
                    heartBeatStrategy.onException(e);
                    throw e;
                } catch (Throwable e) {
                    LOGGER.error("", e);
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        });
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        ScheduleUtil.getTimer().schedule(() -> {
            for (JdbcDataSource value : dataSourceMap.values()) {
                value.close();
            }
        },1,TimeUnit.MINUTES);
    }
}
