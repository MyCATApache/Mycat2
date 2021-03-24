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


import com.alibaba.druid.util.JdbcUtils;
import io.mycat.ConnectionManager;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatException;
import io.mycat.ScheduleUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.config.ClusterConfig;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.ServerConfig;
import io.mycat.datasource.jdbc.DatasourceProvider;
import io.mycat.datasource.jdbc.DruidDatasourceProvider;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.ScheduledHanlde;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.vertx.core.Vertx;
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
    private final ReplicaSelectorManager replicaSelector;

    public JdbcConnectionManager(String customerDatasourceProvider,
                                 Map<String, DatasourceConfig> datasources,
                                 Map<String, ClusterConfig> clusterConfigs,
                                 ReplicaSelectorManager replicaSelector) {
        this(datasources, clusterConfigs, createDatasourceProvider(customerDatasourceProvider), replicaSelector);
    }

    private static DatasourceProvider createDatasourceProvider(String customerDatasourceProvider) {
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        String defaultDatasourceProvider = Optional.ofNullable(customerDatasourceProvider).orElse(DruidDatasourceProvider.class.getName());
        try {
            DatasourceProvider o = (DatasourceProvider) Class.forName(defaultDatasourceProvider)
                    .getDeclaredConstructor().newInstance();
            o.init(serverConfig);
            return o;
        } catch (Exception e) {
            throw new MycatException("can not load datasourceProvider:{}", customerDatasourceProvider);
        }
    }

    public JdbcConnectionManager(Map<String, DatasourceConfig> datasources,
                                 Map<String, ClusterConfig> clusterConfigs,
                                 DatasourceProvider provider,
                                 ReplicaSelectorManager replicaSelector) {
        this.datasourceProvider = Objects.requireNonNull(provider);
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
        JdbcDataSource jdbcDataSource = dataSourceMap.get(key.getName());
        if (jdbcDataSource!=null){
            jdbcDataSource.close();
        }
        dataSourceMap.put(key.getName(), datasourceProvider.createDataSource(key));
    }

    @Override
    public void removeDatasource(String jdbcDataSourceName) {
        JdbcDataSource remove = dataSourceMap.remove(jdbcDataSourceName);
        if (remove != null) {
            remove.close();
        }
    }

    public DefaultConnection getConnection(String name) {
        return getConnection(name, true, Connection.TRANSACTION_REPEATABLE_READ, false);
    }

    public DefaultConnection getConnection(String name, Boolean autocommit,
                                           int transactionIsolation, boolean readOnly) {
        final JdbcDataSource key = Objects.requireNonNull(Optional.ofNullable(dataSourceMap.get(name))
                .orElseGet(() -> {
                    JdbcDataSource jdbcDataSource = dataSourceMap.get(replicaSelector.getDatasourceNameByReplicaName(name, true, null));

                    return jdbcDataSource;
                }), () -> "unknown target:" + name);
        synchronized (key) {
            DefaultConnection defaultConnection;
            Connection connection = null;
            try {
                DatasourceConfig config = key.getConfig();
                connection = key.getDataSource().getConnection();
                defaultConnection = new DefaultConnection(connection, key, autocommit, transactionIsolation, readOnly, this);
                LOGGER.debug("get connection:{} {}", name, defaultConnection);
                if (config.isInitSqlsGetConnection()) {
                    if (config.getInitSqls() != null && !config.getInitSqls().isEmpty()) {
                        try (Statement statement = connection.createStatement()) {
                            for (String initSql : config.getInitSqls()) {
                                statement.execute(initSql);
                            }
                        }
                    }
                }
                key.counter.getAndIncrement();
                return defaultConnection;
            } catch (SQLException e) {
                if (connection != null) {
                    JdbcUtils.close(connection);
                }
                LOGGER.debug("", e);
                throw new MycatException(e);
            }
        }
    }

    @Override
    public void closeConnection(DefaultConnection connection) {
        synchronized (connection.getDataSource()) {
            connection.getDataSource().counter.decrementAndGet();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("close :{} {}", connection, connection.connection);
        }
        //LOGGER.error("{} {}",connection,connection.connection, new Throwable());
        /**
         *
         * To prevent the transaction from being committed at close time,
         * it is implemented in some databases.
         */
        try {
            if (!connection.connection.getAutoCommit()) {
                connection.connection.rollback();
            }
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
        JdbcUtils.close(connection.connection);
    }

    @Override
    public void close() {
        ScheduleUtil.getTimer().schedule(() -> {
            for (JdbcDataSource value : dataSourceMap.values()) {
                value.close();
            }
        }, 30, TimeUnit.SECONDS);

    }

    public Map<String, JdbcDataSource> getDatasourceInfo() {
        return Collections.unmodifiableMap(dataSourceMap);
    }

    private void putHeartFlow(String replicaName, String datasource) {
        replicaSelector.putHeartFlow(replicaName, datasource, new Consumer<HeartBeatStrategy>() {
            @Override
            public void accept(HeartBeatStrategy heartBeatStrategy) {
                Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
                vertx.executeBlocking(promise -> {
                    try {
                        heartbeat(heartBeatStrategy);
                    } catch (Exception e) {
                        heartBeatStrategy.onException(e);
                    }finally {
                        promise.tryComplete();
                    }
                });
            }

            private void heartbeat(HeartBeatStrategy heartBeatStrategy) {
                DefaultConnection connection = null;
                try {
                    connection = getConnection(datasource);
                    ArrayList<List<Map<String, Object>> > resultList = new ArrayList<>();
                    List<String> sqls = heartBeatStrategy.getSqls();
                    for (String sql : sqls) {
                        try (RowBaseIterator iterator  = connection
                                .executeQuery(sql)) {
                            resultList.add( iterator.getResultSetMap());

                        }catch (Exception e){
                            LOGGER.error("jdbc heartbeat ", e);
                            return;
                        }
                    }
                    heartBeatStrategy.process(resultList);
                } catch (Throwable e) {
                    heartBeatStrategy.onException(e);
                    LOGGER.error("", e);
                }  finally {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        });
    }


    public DatasourceProvider getDatasourceProvider() {
        return datasourceProvider;
    }
}
